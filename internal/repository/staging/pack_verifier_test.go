package staging

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/backend/mem"
)

func TestBackendPackVerifierRequiresPhysicalPolicy(t *testing.T) {
	ctx := context.Background()
	contents := []byte("authenticated staged pack")
	digest := sha256.Sum256(contents)
	packID := hex.EncodeToString(digest[:])
	first, second := mem.New(), mem.New()
	handle := backend.Handle{Type: backend.PackFile, Name: packID}
	for _, destination := range []backend.Backend{first, second} {
		if err := destination.Save(ctx, handle, backend.NewByteReader(contents, destination.Hasher())); err != nil {
			t.Fatal(err)
		}
	}
	pack := Pack{
		ID:          packID,
		Type:        "data",
		Size:        int64(len(contents)),
		PayloadSize: 1,
		HeaderSize:  uint64(len(contents) - 1),
		BlobCount:   1,
		SHA256:      packID,
		Placements: []Placement{
			{BackendID: "a", FailureDomain: "site-a", Size: int64(len(contents)), SHA256: packID},
			{BackendID: "b", FailureDomain: "site-b", Offsite: true, Size: int64(len(contents)), SHA256: packID},
		},
	}
	verifier := BackendPackVerifier{Backends: map[string]backend.Backend{"a": first, "b": second}, Policy: Policy{MinCopies: 2, MinDomains: 2, MinOffsite: 1}}
	if err := verifier.VerifyPack(ctx, pack); err != nil {
		t.Fatal(err)
	}
	if err := second.Remove(ctx, handle); err != nil {
		t.Fatal(err)
	}
	if err := verifier.VerifyPack(ctx, pack); err == nil {
		t.Fatal("verification accepted placement below policy")
	}
}
