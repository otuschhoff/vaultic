package main

import (
	"context"
	"testing"

	"github.com/otuschhoff/vaultic/internal/telemetry"
)

func TestPhase34M3CommandOutcomeTreatsNormalServiceStopAsSuccess(t *testing.T) {
	if outcome := classifyCommandOutcome(ErrOK); outcome != telemetry.OutcomeSuccess {
		t.Fatalf("ErrOK outcome = %q", outcome)
	}
	if outcome := classifyCommandOutcome(context.Canceled); outcome != telemetry.OutcomeCancellation {
		t.Fatalf("canceled outcome = %q", outcome)
	}
}
