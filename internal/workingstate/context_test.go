package workingstate

import (
	"context"
	"errors"
	"testing"
)

type fixtureBudget struct{ used, limit uint64 }

func (budget *fixtureBudget) Mode() Mode { return ModeRAM }
func (budget *fixtureBudget) Reserve(size uint64) error {
	if size > budget.limit-budget.used {
		return ErrMemoryLimitExceeded
	}
	budget.used += size
	return nil
}
func (budget *fixtureBudget) Release(size uint64) { budget.used -= size }
func (budget *fixtureBudget) Available() uint64   { return budget.limit - budget.used }

func TestM4LeafReservationOwnership(t *testing.T) {
	budget := &fixtureBudget{limit: 8192}
	ctx := WithPolicy(t.Context(), budget)
	release, err := Reserve(ctx, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if available, selected := Available(ctx); !selected || available != 7168 {
		t.Fatal(available, selected)
	}
	if _, err := Reserve(ctx, 8192); !errors.Is(err, ErrMemoryLimitExceeded) || budget.used != 1024 {
		t.Fatal("failed admission changed ownership", err, budget.used)
	}
	release()
	if budget.used != 0 {
		t.Fatal("leaf lease leaked")
	}
	if _, selected := Available(WithPolicy(ctx, nil)); selected {
		t.Fatal("nil override did not disable policy")
	}
}

func TestM4JSONPreflightOwnership(t *testing.T) {
	for _, scenario := range []struct {
		name   string
		limit  uint64
		input  string
		cancel bool
		want   error
	}{
		{"valid", 16384, `{"items":[1,2,"name"]}`, false, nil},
		{"syntax", 16384, `{"items":[`, false, errors.New("syntax")},
		{"admission", 4200, `{"items":[1,2,"name"]}`, false, ErrMemoryLimitExceeded},
		{"cancel", 16384, `{"items":[]}`, true, context.Canceled},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			budget := &fixtureBudget{limit: scenario.limit}
			ctx, cancel := context.WithCancel(WithPolicy(t.Context(), budget))
			defer cancel()
			if scenario.cancel {
				cancel()
			}
			release, err := ReserveJSON(ctx, []byte(scenario.input))
			if scenario.want == nil {
				if err != nil || budget.used == 0 {
					t.Fatal(err, budget.used)
				}
				release()
			} else {
				if err == nil {
					release()
					t.Fatal("preflight accepted failure")
				}
				if scenario.name != "syntax" && !errors.Is(err, scenario.want) {
					t.Fatal(err)
				}
			}
			if budget.used != 0 {
				t.Fatal("preflight ownership leaked", budget.used)
			}
		})
	}
}
