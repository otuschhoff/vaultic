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
	pauseCounts        []uint64
	pauseBuckets       []float64
}

type WorkingRuntimeInterval struct {
	AllocatedBytes     uint64   `json:"allocated_bytes"`
	GCCycles           uint64   `json:"gc_cycles"`
	GCCPUSeconds       float64  `json:"gc_cpu_seconds"`
	GCAssistCPUSeconds float64  `json:"gc_assist_cpu_seconds"`
	GCPauses           uint64   `json:"gc_pauses"`
	GCPauseP50Seconds  float64  `json:"gc_pause_p50_upper_seconds"`
	GCPauseP95Seconds  float64  `json:"gc_pause_p95_upper_seconds"`
	GCPauseP99Seconds  float64  `json:"gc_pause_p99_upper_seconds"`
	GCPauseMaxSeconds  float64  `json:"gc_pause_max_upper_seconds"`
	Unavailable        []string `json:"unavailable,omitempty"`
}

func WorkingRuntimeDifference(before, after WorkingRuntimeSnapshot) WorkingRuntimeInterval {
	result := WorkingRuntimeInterval{Unavailable: append([]string(nil), before.Unavailable...)}
	result.Unavailable = append(result.Unavailable, after.Unavailable...)
	if after.AllocatedBytes < before.AllocatedBytes || after.GCCycles < before.GCCycles ||
		after.GCCPUSeconds < before.GCCPUSeconds || after.GCAssistCPUSeconds < before.GCAssistCPUSeconds {
		result.Unavailable = append(result.Unavailable, "runtime_counter_reset")
		return result
	}
	result.AllocatedBytes = after.AllocatedBytes - before.AllocatedBytes
	result.GCCycles = after.GCCycles - before.GCCycles
	result.GCCPUSeconds = after.GCCPUSeconds - before.GCCPUSeconds
	result.GCAssistCPUSeconds = after.GCAssistCPUSeconds - before.GCAssistCPUSeconds
	if len(before.pauseCounts) != len(after.pauseCounts) || len(after.pauseBuckets) != len(after.pauseCounts)+1 {
		result.Unavailable = append(result.Unavailable, workingRuntimeNames[8])
		return result
	}
	counts := make([]uint64, len(after.pauseCounts))
	for index, count := range after.pauseCounts {
		if count < before.pauseCounts[index] || len(before.pauseBuckets) != len(after.pauseBuckets) ||
			before.pauseBuckets[index] != after.pauseBuckets[index] || before.pauseBuckets[index+1] != after.pauseBuckets[index+1] {
			result.Unavailable = append(result.Unavailable, workingRuntimeNames[8])
			return result
		}
		counts[index] = count - before.pauseCounts[index]
		result.GCPauses = saturatingWorkingAdd(result.GCPauses, counts[index])
	}
	percentiles := []struct {
		percent uint64
		output  *float64
	}{{50, &result.GCPauseP50Seconds}, {95, &result.GCPauseP95Seconds},
		{99, &result.GCPauseP99Seconds}, {100, &result.GCPauseMaxSeconds}}
	for _, percentile := range percentiles {
		target := result.GCPauses - result.GCPauses/100*(100-percentile.percent) - result.GCPauses%100*(100-percentile.percent)/100
		var seen uint64
		for index, count := range counts {
			seen = saturatingWorkingAdd(seen, count)
			if target == 0 || seen < target {
				continue
			}
			upper := after.pauseBuckets[index+1]
			if math.IsInf(upper, 0) || math.IsNaN(upper) || upper < 0 {
				result.Unavailable = append(result.Unavailable, workingRuntimeNames[8])
			} else {
				*percentile.output = upper
			}
			break
		}
	}
	return result
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
			result.pauseCounts = append([]uint64(nil), histogram.Counts...)
			result.pauseBuckets = append([]float64(nil), histogram.Buckets...)
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
