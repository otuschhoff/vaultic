package daemon

import (
	"encoding/binary"
	"math"
	"sync"

	"github.com/otuschhoff/vaultic/internal/index/schema"
)

const seenFilterGrowth = 2

type idSeenFilter struct {
	mu                  sync.RWMutex
	layers              []idSeenFilterLayer
	initialCapacity     uint64
	maxBytes            uint64
	targetFalsePositive float64
	allocatedBytes      uint64
	inserts             uint64
	fallbackToDatabase  bool
}

type idSeenFilterLayer struct {
	bits          []byte
	capacity      uint64
	inserts       uint64
	probes        uint8
	falsePositive float64
}

type idSeenFilterStats struct {
	Layers                 uint64
	Bytes                  uint64
	Inserts                uint64
	EstimatedFalsePositive float64
	FallbackToDatabase     bool
	LayerOccupancy         []float64
}

func newIDSeenFilter(initialCapacity int, maxBytes int, targetFalsePositive float64) *idSeenFilter {
	filter := &idSeenFilter{
		initialCapacity:     uint64(max(initialCapacity, 1)),
		maxBytes:            uint64(max(maxBytes, 1)),
		targetFalsePositive: targetFalsePositive,
	}
	if !filter.addLayer() {
		filter.fallbackToDatabase = true
	}
	return filter
}

func (filter *idSeenFilter) possiblyContains(id schema.ID) bool {
	filter.mu.RLock()
	defer filter.mu.RUnlock()
	if filter.fallbackToDatabase {
		return true
	}
	for index := len(filter.layers) - 1; index >= 0; index-- {
		if filter.layers[index].possiblyContains(id) {
			return true
		}
	}
	return false
}

func (filter *idSeenFilter) insert(id schema.ID) {
	filter.mu.Lock()
	defer filter.mu.Unlock()
	if filter.fallbackToDatabase {
		return
	}
	if len(filter.layers) == 0 {
		filter.fallbackToDatabase = true
		return
	}
	for index := len(filter.layers) - 1; index >= 0; index-- {
		if filter.layers[index].possiblyContains(id) {
			return
		}
	}
	active := &filter.layers[len(filter.layers)-1]
	if active.inserts >= active.capacity {
		if !filter.addLayer() {
			filter.fallbackToDatabase = true
			return
		}
		active = &filter.layers[len(filter.layers)-1]
	}
	active.insert(id)
	active.inserts++
	filter.inserts++
}

func (filter *idSeenFilter) stats() idSeenFilterStats {
	filter.mu.RLock()
	defer filter.mu.RUnlock()
	probabilityAbsent := 1.0
	occupancy := make([]float64, 0, len(filter.layers))
	for _, layer := range filter.layers {
		probabilityAbsent *= 1 - layer.estimatedFalsePositive()
		occupancy = append(occupancy, float64(layer.inserts)/float64(layer.capacity))
	}
	return idSeenFilterStats{
		Layers: uint64(len(filter.layers)), Bytes: filter.allocatedBytes, Inserts: filter.inserts,
		EstimatedFalsePositive: 1 - probabilityAbsent, FallbackToDatabase: filter.fallbackToDatabase,
		LayerOccupancy: occupancy,
	}
}

func (filter *idSeenFilter) addLayer() bool {
	if filter.targetFalsePositive <= 0 || filter.targetFalsePositive >= 1 || math.IsNaN(filter.targetFalsePositive) {
		return false
	}
	capacity := filter.initialCapacity
	for range len(filter.layers) {
		if capacity > math.MaxUint64/seenFilterGrowth {
			return false
		}
		capacity *= seenFilterGrowth
	}
	falsePositive := filter.targetFalsePositive / math.Pow(2, float64(len(filter.layers)+1))
	byteCount, probeCount := seenFilterLayerParameters(capacity, falsePositive)
	if byteCount == 0 || probeCount == 0 || filter.allocatedBytes >= filter.maxBytes ||
		byteCount > filter.maxBytes-filter.allocatedBytes {
		return false
	}
	filter.layers = append(filter.layers, idSeenFilterLayer{
		bits: make([]byte, byteCount), capacity: capacity, probes: probeCount, falsePositive: falsePositive,
	})
	filter.allocatedBytes += byteCount
	return true
}

func seenFilterLayerParameters(capacity uint64, falsePositive float64) (uint64, uint8) {
	if capacity == 0 || falsePositive <= 0 || falsePositive >= 1 || math.IsNaN(falsePositive) {
		return 0, 0
	}
	bitsPerItem := -math.Log(falsePositive) / (math.Ln2 * math.Ln2)
	bitCountFloat := math.Ceil(float64(capacity) * bitsPerItem)
	if math.IsInf(bitCountFloat, 0) || bitCountFloat > math.MaxUint64-7 {
		return 0, 0
	}
	bitCount := uint64(bitCountFloat)
	byteCount := (bitCount + 7) / 8
	probeCount := math.Round(bitsPerItem * math.Ln2)
	probeCount = min(max(probeCount, 1), float64(math.MaxUint8))
	return byteCount, uint8(probeCount)
}

func (layer *idSeenFilterLayer) possiblyContains(id schema.ID) bool {
	first, second := seenFilterHashes(id)
	bitCount := uint64(len(layer.bits)) * 8
	for probe := uint64(0); probe < uint64(layer.probes); probe++ {
		bit := (first + probe*second) % bitCount
		if layer.bits[bit/8]&(1<<uint(bit%8)) == 0 {
			return false
		}
	}
	return true
}

func (layer *idSeenFilterLayer) insert(id schema.ID) {
	first, second := seenFilterHashes(id)
	bitCount := uint64(len(layer.bits)) * 8
	for probe := uint64(0); probe < uint64(layer.probes); probe++ {
		bit := (first + probe*second) % bitCount
		layer.bits[bit/8] |= 1 << uint(bit%8)
	}
}

func (layer *idSeenFilterLayer) estimatedFalsePositive() float64 {
	if layer.inserts == 0 {
		return 0
	}
	bits := float64(len(layer.bits) * 8)
	probes := float64(layer.probes)
	return math.Pow(1-math.Exp(-probes*float64(layer.inserts)/bits), probes)
}

func seenFilterHashes(id schema.ID) (uint64, uint64) {
	first := binary.BigEndian.Uint64(id[:8]) ^ binary.BigEndian.Uint64(id[16:24])
	second := binary.BigEndian.Uint64(id[8:16]) ^ binary.BigEndian.Uint64(id[24:])
	second = second*0x9e3779b97f4a7c15 | 1
	return first, second
}
