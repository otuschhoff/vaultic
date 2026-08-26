package pack

import "github.com/otuschhoff/vaultic/internal/vaultic"

// PackedBlob is one index entry for a blob in a pack (may be duplicate across indexes).
type PackedBlob struct {
	Pack vaultic.ID
	Blob Blob
}

var _ vaultic.PackBlob = (*PackedBlob)(nil)

func (pb *PackedBlob) PackID() vaultic.ID { return pb.Pack }

func (pb *PackedBlob) Handle() vaultic.BlobHandle { return pb.Blob.BlobHandle }

func (pb *PackedBlob) CiphertextLength() uint { return pb.Blob.Length }

func (pb *PackedBlob) UncompressedCiphertextLength() uint {
	return pb.Blob.UncompressedCiphertextLength()
}

func (pb *PackedBlob) PlaintextLength() uint { return pb.Blob.DataLength() }

func (pb *PackedBlob) IsCompressed() bool { return pb.Blob.IsCompressed() }
