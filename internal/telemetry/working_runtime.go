package telemetry

import (
	"math"
	runtimemetrics "runtime/metrics"
)

type WorkingRuntimeSnapshot struct {
	HeapBytes          uint64   `json:"heap_bytes"`
	HeapObjects        uint64   `json:"heap_objects"`
	AllocatedBytes     uint64   `json:"allocated_bytes_total"`
	ScannableHeapBytes uint64   `json:"scannable_heap_bytes"`
	RuntimeBytes       uint64   `json:"runtime_bytes"`
	GCCycles           uint64   `json:"gc_cycles_total"`
	GCCPUSeconds       float64  `json:"gc_cpu_seconds_total"`
	GCAssistCPUSeconds float64  `json:"gc_assist_cpu_seconds_total"`
	GCPauses           uint64   `json:"gc_pauses_total"`
	GCPauseP99Seconds  float64  `json:"gc_pause_p99_seconds_cumulative"`
	Unavailable        []string `json:"unavailable,omitempty"`
}

var workingRuntimeNames = [...]string{
	"/memory/classes/heap/objects:bytes",
	"/gc/heap/objects:objects",
	"/gc/heap/allocs:bytes",
	"/gc/scan/heap:bytes",
	"/memory/classes/total:bytes",
	"/gc/cycles/total:gc-cycles",
	"/cpu/classes/gc/total:cpu-seconds",
	"/cpu/classes/gc/mark/assist:cpu-seconds",
	"/sched/pauses/total/gc:seconds",
}

func ReadWorkingRuntime() WorkingRuntimeSnapshot {
	samples := make([]runtimemetrics.Sample, len(workingRuntimeNames))
	for index, name := range workingRuntimeNames {
		samples[index].Name = name
	}
	runtimemetrics.Read(samples)
	return workingRuntimeSnapshot(samples)
}

func workingRuntimeSnapshot(samples []runtimemetrics.Sample) WorkingRuntimeSnapshot {
	var result WorkingRuntimeSnapshot
	integers := []*uint64{&result.HeapBytes, &result.HeapObjects, &result.AllocatedBytes,
		&result.ScannableHeapBytes, &result.RuntimeBytes, &result.GCCycles}
	for index, sample := range samples {
		switch {
		case index < len(integers) && sample.Value.Kind() == runtimemetrics.KindUint64:
			*integers[index] = sample.Value.Uint64()
		case index == 6 && sample.Value.Kind() == runtimemetrics.KindFloat64:
			result.GCCPUSeconds = sample.Value.Float64()
		case index == 7 && sample.Value.Kind() == runtimemetrics.KindFloat64:
			result.GCAssistCPUSeconds = sample.Value.Float64()
		case index == 8 && sample.Value.Kind() == runtimemetrics.KindFloat64Histogram:
			histogram := sample.Value.Float64Histogram()
			for _, count := range histogram.Counts {
				result.GCPauses = saturatingWorkingAdd(result.GCPauses, count)
			}
			target := result.GCPauses - result.GCPauses/100
			var seen uint64
			for bucket, count := range histogram.Counts {
				seen = saturatingWorkingAdd(seen, count)
				if target != 0 && seen >= target {
					upper := histogram.Buckets[bucket+1]
					if !math.IsInf(upper, 0) && !math.IsNaN(upper) {
						result.GCPauseP99Seconds = upper
					} else {
						result.Unavailable = append(result.Unavailable, sample.Name)
					}
					break
				}
			}
		default:
			result.Unavailable = append(result.Unavailable, sample.Name)
		}
	}
	return result
}
