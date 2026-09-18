package telemetry

import (
	"sync/atomic"
	"testing"
)

func TestCorrelationSamplerIsDeterministicBoundedAndCountsDrops(t *testing.T) {
	sampler := NewCorrelationSampler("legacy_import", "database", 2, 1, true)
	var now atomic.Int64
	now.Store(1)
	sampler.now = now.Load
	unsampled := sampler.Start("")
	unsampled.Succeeded()
	unsampled.Done()
	first := sampler.Start("parent-1")
	now.Store(11)
	first.Succeeded()
	first.Done()
	secondUnsampled := sampler.Start("")
	secondUnsampled.Done()
	second := sampler.Start("")
	second.Failed()
	second.Done()
	if sampler.Dropped() != 1 {
		t.Fatalf("dropped = %d", sampler.Dropped())
	}
	correlations := sampler.Drain(10)
	if len(correlations) != 1 || correlations[0].ParentID != "parent-1" || correlations[0].Outcome != OutcomeSuccess || correlations[0].DurationUS != 10 {
		t.Fatalf("correlations = %+v", correlations)
	}
}

func TestCorrelationSamplerRejectsUnsafeParentAndDisabledPathDoesNotAllocate(t *testing.T) {
	sampler := NewCorrelationSampler("check", "scratch", 1, 1, true)
	guard := sampler.Start("/private/path")
	guard.Done()
	if len(sampler.Drain(1)) != 0 {
		t.Fatal("unsafe parent was sampled")
	}
	disabled := NewCorrelationSampler("check", "scratch", 1, 1, false)
	if allocations := testing.AllocsPerRun(100, func() {
		guard := disabled.Start("")
		guard.Succeeded()
		guard.Done()
	}); allocations != 0 {
		t.Fatalf("disabled correlation allocations = %v", allocations)
	}
}

func TestCorrelationSamplerRetainedStateIsBounded(t *testing.T) {
	sampler := NewCorrelationSampler("check", "database", 1, MaxSampledCorrelations, true)
	if cap(sampler.completed) != MaxSampledCorrelations {
		t.Fatalf("correlation capacity = %d, want %d", cap(sampler.completed), MaxSampledCorrelations)
	}
}

func BenchmarkCorrelationAccounting(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		b.Run(map[bool]string{false: "disabled", true: "enabled"}[enabled], func(b *testing.B) {
			sampler := NewCorrelationSampler("check", "database", 1, 1, enabled)
			b.ReportAllocs()
			for b.Loop() {
				guard := sampler.Start("")
				guard.Succeeded()
				guard.Done()
				if enabled {
					sample := sampler.Drain(1)
					_ = sample
				}
			}
		})
	}
}
