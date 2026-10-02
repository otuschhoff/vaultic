package workingstate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
)

type Mode string

var ErrMemoryLimitExceeded = errors.New("working memory limit exceeded")

const (
	ModeRAM  Mode = "ram"
	ModeKV   Mode = "kv"
	ModeAuto Mode = "auto"
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

func Reserve(ctx context.Context, size uint64) (func(), error) {
	policy, ok := PolicyFrom(ctx).(interface {
		Reserve(uint64) error
		Release(uint64)
	})
	if !ok {
		return func() {}, nil
	}
	if err := policy.Reserve(size); err != nil {
		return nil, err
	}
	return func() { policy.Release(size) }, nil
}

func ReserveJSON(ctx context.Context, encoded []byte) (func(), error) {
	policy, ok := PolicyFrom(ctx).(interface {
		Reserve(uint64) error
		Release(uint64)
	})
	if !ok {
		return func() {}, nil
	}
	charged := uint64(cap(encoded))*3 + 4096
	if err := policy.Reserve(charged); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		policy.Release(charged)
		return nil, err
	}
	if !json.Valid(encoded) {
		policy.Release(charged)
		return nil, errors.New("invalid working-state JSON")
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	for {
		if err := ctx.Err(); err != nil {
			policy.Release(charged)
			return nil, err
		}
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			policy.Release(charged)
			return nil, err
		}
		charge := uint64(128)
		if delimiter, ok := token.(json.Delim); ok {
			if delimiter == '}' || delimiter == ']' {
				continue
			}
			if delimiter == '{' {
				charge = 768
			}
		}
		if err := policy.Reserve(charge); err != nil {
			policy.Release(charged)
			return nil, err
		}
		charged += charge
	}
	return func() { policy.Release(charged) }, nil
}

func Available(ctx context.Context) (uint64, bool) {
	policy, ok := PolicyFrom(ctx).(interface{ Available() uint64 })
	if !ok {
		return 0, false
	}
	return policy.Available(), true
}
