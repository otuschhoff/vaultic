package staging

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"

	"github.com/otuschhoff/vaultic/internal/backend"
)

type BackendPackVerifier struct {
	Backends map[string]backend.Backend
	Policy   Policy
}

func (verifier BackendPackVerifier) VerifyPack(ctx context.Context, pack Pack) error {
	verified := uint(0)
	offsite := uint(0)
	domains := make(map[string]struct{})
	for _, placement := range pack.Placements {
		destination, ok := verifier.Backends[placement.BackendID]
		if !ok {
			continue
		}
		handle := backend.Handle{Type: backend.PackFile, Name: pack.ID, IsMetadata: pack.Type == "tree"}
		info, err := destination.Stat(ctx, handle)
		if err != nil || info.Size != pack.Size || info.Size != placement.Size {
			continue
		}
		hash := sha256.New()
		err = destination.Load(ctx, handle, int(info.Size), 0, func(reader io.Reader) error {
			read, err := io.Copy(hash, io.LimitReader(reader, info.Size+1))
			if err != nil {
				return err
			}
			if read != info.Size {
				return fmt.Errorf("short staged pack read")
			}
			return nil
		})
		if err != nil || hex.EncodeToString(hash.Sum(nil)) != pack.SHA256 || pack.SHA256 != placement.SHA256 || pack.ID != pack.SHA256 {
			continue
		}
		verified++
		domains[placement.FailureDomain] = struct{}{}
		if placement.Offsite {
			offsite++
		}
	}
	if verified < verifier.Policy.MinCopies || uint(len(domains)) < verifier.Policy.MinDomains || offsite < verifier.Policy.MinOffsite {
		return Retryable(fmt.Errorf("staged pack %s has insufficient verified placement", pack.ID))
	}
	return nil
}
