package telemetry

import "sync"

type WorkingStateKind string

const (
	WorkingWrittenBlobs WorkingStateKind = "written_blobs"
	WorkingMarkers      WorkingStateKind = "markers"
	WorkingDirectories  WorkingStateKind = "directories"
	WorkingCheck        WorkingStateKind = "check_spool"
	WorkingImport       WorkingStateKind = "import"
)

type WorkingStateSnapshot struct {
	Kind                       WorkingStateKind `json:"kind"`
	Backend                    string           `json:"backend"`
	CommittedEntries           uint64           `json:"committed_entries"`
	CommittedEncodedBytes      uint64           `json:"committed_encoded_bytes"`
	RetainedEntriesUpperBound  uint64           `json:"retained_entries_upper_bound"`
	RetainedEncodedBytesBound  uint64           `json:"retained_encoded_bytes_upper_bound"`
	ObservedBufferBytes        uint64           `json:"observed_buffer_bytes"`
	PeakObservedBufferBytes    uint64           `json:"peak_observed_buffer_bytes"`
	ReservedScratchBytes       uint64           `json:"reserved_scratch_bytes"`
	PeakReservedScratchBytes   uint64           `json:"peak_reserved_scratch_bytes"`
	ScratchReservationEnforced bool             `json:"scratch_reservation_enforced"`
	ScratchBytes               uint64           `json:"scratch_bytes"`
	PeakScratchBytesObserved   uint64           `json:"peak_scratch_bytes_observed"`
	ScratchBytesKnown          bool             `json:"scratch_bytes_known"`
	ReservationEnforced        bool             `json:"reservation_enforced"`
	Closed                     bool             `json:"closed"`
	Activated                  bool             `json:"activated"`
}

type WorkingStateMetric struct {
	mutex sync.Mutex
	state WorkingStateSnapshot
}

func NewWorkingStateMetric(kind WorkingStateKind, backend string) *WorkingStateMetric {
	switch kind {
	case WorkingWrittenBlobs, WorkingMarkers, WorkingDirectories, WorkingCheck, WorkingImport:
	default:
		panic("unsupported working-state kind")
	}
	switch backend {
	case "pebble", "bbolt", "ram", "encrypted_sort", "streaming":
	default:
		panic("unsupported working-state backend")
	}
	return &WorkingStateMetric{state: WorkingStateSnapshot{Kind: kind, Backend: backend}}
}

func (metric *WorkingStateMetric) Committed(entries, encodedBytes uint64) {
	metric.mutex.Lock()
	defer metric.mutex.Unlock()
	if metric.state.Closed {
		return
	}
	metric.state.CommittedEntries = saturatingWorkingAdd(metric.state.CommittedEntries, entries)
	metric.state.CommittedEncodedBytes = saturatingWorkingAdd(metric.state.CommittedEncodedBytes, encodedBytes)
	metric.state.RetainedEntriesUpperBound = metric.state.CommittedEntries
	metric.state.RetainedEncodedBytesBound = metric.state.CommittedEncodedBytes
}

func (metric *WorkingStateMetric) Activate() {
	metric.mutex.Lock()
	defer metric.mutex.Unlock()
	if !metric.state.Closed {
		metric.state.Activated = true
	}
}

func (metric *WorkingStateMetric) ObserveBuffer(bytes uint64) {
	metric.mutex.Lock()
	defer metric.mutex.Unlock()
	if metric.state.Closed {
		return
	}
	metric.state.ObservedBufferBytes = bytes
	metric.state.PeakObservedBufferBytes = max(metric.state.PeakObservedBufferBytes, bytes)
}

func (metric *WorkingStateMetric) Snapshot(scratchBytes uint64, known bool) WorkingStateSnapshot {
	metric.mutex.Lock()
	defer metric.mutex.Unlock()
	if known {
		metric.state.PeakScratchBytesObserved = max(metric.state.PeakScratchBytesObserved, scratchBytes)
	}
	state := metric.state
	state.ScratchBytes, state.ScratchBytesKnown = scratchBytes, known
	return state
}

func (metric *WorkingStateMetric) Close() {
	metric.mutex.Lock()
	defer metric.mutex.Unlock()
	metric.state.Closed = true
	metric.state.RetainedEntriesUpperBound = 0
	metric.state.RetainedEncodedBytesBound = 0
	metric.state.ObservedBufferBytes = 0
}

func saturatingWorkingAdd(left, right uint64) uint64 {
	if ^uint64(0)-left < right {
		return ^uint64(0)
	}
	return left + right
}
