package workingstate

import "context"

type Mode string

const (
	ModeRAM Mode = "ram"
	ModeKV  Mode = "kv"
)

type Policy interface{ Mode() Mode }

type policyKey struct{}
type policyValue struct{ policy Policy }

func WithPolicy(ctx context.Context, policy Policy) context.Context {
	return context.WithValue(ctx, policyKey{}, policyValue{policy: policy})
}

func PolicyFrom(ctx context.Context) Policy {
	value, _ := ctx.Value(policyKey{}).(policyValue)
	return value.policy
}
