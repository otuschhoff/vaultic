package maintenance

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
)

func checkVerificationState(
	ctx context.Context,
	store Store,
	result *CheckResult,
	maxFindings uint,
) error {
	if err := scan(ctx, store, schema.VerificationStatePrefix(), func(kv daemon.KeyValue) error {
		key, err := schema.ParseKey(kv.Key)
		if err != nil {
			return err
		}
		state, err := schema.UnmarshalVerificationStateRecord(kv.Value)
		if err != nil {
			result.VerificationStateMismatch++
			addFinding(result, maxFindings, Finding{Kind: "verification_state_malformed", Key: hex.EncodeToString(kv.Key), Got: err.Error()})
			return nil
		}
		if _, found, err := store.Get(ctx, schema.PackKey(key.ID)); err != nil {
			return err
		} else if !found {
			result.VerificationStateMismatch++
			addFinding(result, maxFindings, Finding{Kind: "verification_state_orphan_pack", Key: verificationPlacementKey(key.ID, key.Backend)})
		}
		value, found, err := store.Get(ctx, schema.PackPlacementKey(key.ID, key.Backend))
		if err != nil {
			return err
		}
		if !found {
			result.VerificationStateMismatch++
			addFinding(result, maxFindings, Finding{Kind: "verification_state_orphan_placement", Key: verificationPlacementKey(key.ID, key.Backend)})
			return nil
		}
		placement, err := schema.UnmarshalPlacementRecord(value)
		if err != nil {
			return err
		}
		if placement.LastVerifiedAt != state.HeaderVerifiedAt {
			result.VerificationStateMismatch++
			addFinding(result,
				maxFindings,
				Finding{Kind: ("verification_projection_mismatch"),
					Key: verificationPlacementKey(key.ID,
						key.Backend),
					Want: fmt.Sprint(state.HeaderVerifiedAt),
					Got:  fmt.Sprint(placement.LastVerifiedAt)})
		}
		return nil
	}); err != nil {
		return err
	}
	return scan(ctx, store, []byte("pl:"), func(kv daemon.KeyValue) error {
		key, err := schema.ParseKey(kv.Key)
		if err != nil {
			return err
		}
		placement, err := schema.UnmarshalPlacementRecord(kv.Value)
		if err != nil {
			return err
		}
		if placement.LastVerifiedAt != 0 {
			value, found, getErr := store.Get(ctx, schema.VerificationStateKey(key.ID, key.Backend))
			if getErr != nil {
				return getErr
			}
			valid := false
			if found {
				_, decodeErr := schema.UnmarshalVerificationStateRecord(value)
				valid = decodeErr == nil
			}
			if !valid {
				result.VerificationStateMismatch++
				addFinding(
					result,
					maxFindings,
					Finding{
						Kind: "verification_state_missing",
						Key:  verificationPlacementKey(key.ID, key.Backend),
						Want: fmt.Sprint(placement.LastVerifiedAt),
					},
				)
			}
		}
		return nil
	})
}

func verificationPlacementKey(pack schema.ID, backend uint64) string {
	return hex.EncodeToString(pack[:]) + ":" + fmt.Sprint(backend)
}
