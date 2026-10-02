package workingkv

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"unsafe"
)

var ErrWorkingMemoryLimitExceeded = errors.New("working memory limit exceeded")

type MemoryLimitError struct{ Limit, Used, Requested uint64 }

func (err *MemoryLimitError) Error() string {
	return fmt.Sprintf("working memory limit exceeded: limit=%d used=%d requested=%d", err.Limit, err.Used, err.Requested)
}
func (err *MemoryLimitError) Is(target error) bool { return target == ErrWorkingMemoryLimitExceeded }

type BudgetSnapshot struct{ Limit, Used, Peak uint64 }
type Budget struct {
	mutex sync.Mutex
	state BudgetSnapshot
}

func NewBudget(limit uint64) (*Budget, error) {
	if limit == 0 {
		return nil, ErrInvalid
	}
	return &Budget{state: BudgetSnapshot{Limit: limit}}, nil
}
func (budget *Budget) Snapshot() BudgetSnapshot {
	budget.mutex.Lock()
	defer budget.mutex.Unlock()
	return budget.state
}
func (budget *Budget) reserve(size uint64) error {
	budget.mutex.Lock()
	defer budget.mutex.Unlock()
	if size > budget.state.Limit-budget.state.Used {
		return &MemoryLimitError{budget.state.Limit, budget.state.Used, size}
	}
	budget.state.Used += size
	budget.state.Peak = max(budget.state.Peak, budget.state.Used)
	return nil
}
func (budget *Budget) release(size uint64) {
	budget.mutex.Lock()
	defer budget.mutex.Unlock()
	if size > budget.state.Used {
		panic("working memory reservation underflow")
	}
	budget.state.Used -= size
}

const ramChunkBytes = 64 << 10
const ramNodeCount = 256
const ramBaseCharge = 1024

type ramSpan struct{ chunk, start, size uint32 }
type ramNode struct {
	key, value            ramSpan
	left, right           uint32
	height, valueCapacity uint32
}
type ramChunk struct {
	data []byte
	used uint32
	live uint64
}
type ramEngine struct {
	mutex             sync.RWMutex
	budget            *Budget
	chunks            []ramChunk
	nodes             [][]ramNode
	count, root, tail uint32
	charged           uint64
}

func ramCharge(size uint64) uint64 { return size + size/4 + 64 }
func OpenRAM(budget *Budget) (*Store, error) {
	if budget == nil {
		return nil, ErrInvalid
	}
	if err := budget.reserve(ramBaseCharge); err != nil {
		return nil, err
	}
	impl := &ramEngine{budget: budget, charged: ramBaseCharge}
	return &Store{engine: impl, writer: make(chan struct{}, 1), done: make(chan struct{})}, nil
}

func (impl *ramEngine) node(index uint32) *ramNode {
	index--
	return &impl.nodes[index/ramNodeCount][index%ramNodeCount]
}
func (impl *ramEngine) data(span ramSpan) []byte {
	if span.size == 0 {
		return nil
	}
	return impl.chunks[span.chunk].data[span.start : span.start+span.size]
}
func (impl *ramEngine) find(key []byte) uint32 {
	for index := impl.root; index != 0; {
		node := impl.node(index)
		order := bytes.Compare(key, impl.data(node.key))
		if order == 0 {
			return index
		}
		if order < 0 {
			index = node.left
		} else {
			index = node.right
		}
	}
	return 0
}
func (impl *ramEngine) height(index uint32) uint32 {
	if index == 0 {
		return 0
	}
	return impl.node(index).height
}
func (impl *ramEngine) refresh(index uint32) {
	node := impl.node(index)
	node.height = 1 + max(impl.height(node.left), impl.height(node.right))
}
func (impl *ramEngine) rotateLeft(index uint32) uint32 {
	root := impl.node(index).right
	impl.node(index).right = impl.node(root).left
	impl.node(root).left = index
	impl.refresh(index)
	impl.refresh(root)
	return root
}
func (impl *ramEngine) rotateRight(index uint32) uint32 {
	root := impl.node(index).left
	impl.node(index).left = impl.node(root).right
	impl.node(root).right = index
	impl.refresh(index)
	impl.refresh(root)
	return root
}
func (impl *ramEngine) insert(root, index uint32) uint32 {
	if root == 0 {
		return index
	}
	if bytes.Compare(impl.data(impl.node(index).key), impl.data(impl.node(root).key)) < 0 {
		impl.node(root).left = impl.insert(impl.node(root).left, index)
	} else {
		impl.node(root).right = impl.insert(impl.node(root).right, index)
	}
	impl.refresh(root)
	if int(impl.height(impl.node(root).left))-int(impl.height(impl.node(root).right)) > 1 {
		left := impl.node(root).left
		if impl.height(impl.node(left).right) > impl.height(impl.node(left).left) {
			impl.node(root).left = impl.rotateLeft(left)
		}
		return impl.rotateRight(root)
	}
	if int(impl.height(impl.node(root).right))-int(impl.height(impl.node(root).left)) > 1 {
		right := impl.node(root).right
		if impl.height(impl.node(right).left) > impl.height(impl.node(right).right) {
			impl.node(root).right = impl.rotateRight(right)
		}
		return impl.rotateLeft(root)
	}
	return root
}

func (impl *ramEngine) put(ctx context.Context, entries []Entry) error {
	impl.mutex.Lock()
	defer impl.mutex.Unlock()
	planCharge := ramCharge(uint64(len(entries))*8) + 512
	if err := impl.budget.reserve(planCharge); err != nil {
		return err
	}
	defer impl.budget.release(planCharge)
	order := make([]uint32, len(entries))
	for ordinal := range order {
		order[ordinal] = uint32(ordinal)
	}
	sort.Slice(order, func(left, right int) bool {
		compared := bytes.Compare(entries[order[left]].Key, entries[order[right]].Key)
		return compared < 0 || compared == 0 && order[left] < order[right]
	})
	indexes := make([]uint32, len(entries))
	used, newKeys := 0, uint64(0)
	for first := 0; first < len(order); {
		if err := ctx.Err(); err != nil {
			return err
		}
		last := first + 1
		for last < len(order) && bytes.Equal(entries[order[first]].Key, entries[order[last]].Key) {
			last++
		}
		ordinal := order[last-1]
		index := impl.find(entries[ordinal].Key)
		order[used], indexes[used] = ordinal, index
		used++
		if index == 0 {
			newKeys++
		}
		first = last
	}
	if uint64(impl.count)+newKeys > uint64(^uint32(0))-1 {
		return ErrWorkingMemoryLimitExceeded
	}
	available := 0
	if impl.tail != 0 {
		tail := impl.chunks[impl.tail-1]
		if tail.data != nil {
			available = len(tail.data) - int(tail.used)
		}
	}
	newChunks, arenaCharge := 0, uint64(0)
	for ordinal := range used {
		entry := entries[order[ordinal]]
		index := indexes[ordinal]
		if err := ctx.Err(); err != nil {
			return err
		}
		key, value := entry.Key, entry.Value
		if index != 0 {
			key = nil
			if uint32(len(value)) <= impl.node(index).valueCapacity {
				value = nil
			}
		}
		for _, value := range [][]byte{key, value} {
			if len(value) > available {
				capacity := max(ramChunkBytes, len(value))
				newChunks++
				arenaCharge += ramCharge(uint64(capacity))
				available = capacity
			}
			available -= len(value)
		}
	}
	neededNodes := (uint64(impl.count) + newKeys + ramNodeCount - 1) / ramNodeCount
	newNodes := int(neededNodes) - len(impl.nodes)
	holes := 0
	for _, chunk := range impl.chunks {
		if chunk.data == nil {
			holes++
		}
	}
	chunkCapacity := cap(impl.chunks)
	if len(impl.chunks)+max(0, newChunks-holes) > chunkCapacity {
		chunkCapacity = max(len(impl.chunks)+max(0, newChunks-holes), 2*chunkCapacity, 4)
	}
	nodeCapacity := cap(impl.nodes)
	if int(neededNodes) > nodeCapacity {
		nodeCapacity = max(int(neededNodes), 2*nodeCapacity, 4)
	}
	reservation := arenaCharge + uint64(newNodes)*ramCharge(uint64(ramNodeCount)*uint64(unsafe.Sizeof(ramNode{})))
	if chunkCapacity != cap(impl.chunks) {
		reservation += ramCharge(uint64(chunkCapacity) * uint64(unsafe.Sizeof(ramChunk{})))
	}
	if nodeCapacity != cap(impl.nodes) {
		reservation += ramCharge(uint64(nodeCapacity) * uint64(unsafe.Sizeof([]ramNode{})))
	}
	if err := impl.budget.reserve(reservation); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		impl.budget.release(reservation)
		return err
	}
	if chunkCapacity != cap(impl.chunks) {
		grown := make([]ramChunk, len(impl.chunks), chunkCapacity)
		copy(grown, impl.chunks)
		impl.chunks = grown
	}
	if nodeCapacity != cap(impl.nodes) {
		grown := make([][]ramNode, len(impl.nodes), nodeCapacity)
		copy(grown, impl.nodes)
		impl.nodes = grown
	}
	for range newNodes {
		impl.nodes = append(impl.nodes, make([]ramNode, ramNodeCount))
	}
	previousCharge := impl.charged
	for ordinal := range used {
		entry := entries[order[ordinal]]
		index := indexes[ordinal]
		if index != 0 {
			node := impl.node(index)
			if node.valueCapacity >= uint32(len(entry.Value)) {
				span := node.value
				span.size = node.valueCapacity
				copy(impl.data(span), entry.Value)
				node.value.size = uint32(len(entry.Value))
				continue
			}
		}
		var key ramSpan
		if index == 0 {
			key = impl.appendBytes(entry.Key)
		}
		value := impl.appendBytes(entry.Value)
		if index != 0 {
			node := impl.node(index)
			if node.valueCapacity != 0 {
				chunk := &impl.chunks[node.value.chunk]
				chunk.live -= uint64(node.valueCapacity)
				if chunk.live == 0 {
					chunk.data = nil
					chunk.used = 0
				}
			}
			node.value = value
			node.valueCapacity = uint32(len(entry.Value))
			continue
		}
		impl.count++
		index = impl.count
		*impl.node(index) = ramNode{key: key, value: value, height: 1, valueCapacity: uint32(len(entry.Value))}
		impl.root = impl.insert(impl.root, index)
	}
	impl.charged = impl.retainedCharge()
	impl.budget.release(previousCharge + reservation - impl.charged)
	return nil
}

func (impl *ramEngine) retainedCharge() uint64 {
	charge := uint64(ramBaseCharge)
	for _, chunk := range impl.chunks {
		if chunk.data != nil {
			charge += ramCharge(uint64(len(chunk.data)))
		}
	}
	charge += uint64(len(impl.nodes)) * ramCharge(uint64(ramNodeCount)*uint64(unsafe.Sizeof(ramNode{})))
	if cap(impl.chunks) != 0 {
		charge += ramCharge(uint64(cap(impl.chunks)) * uint64(unsafe.Sizeof(ramChunk{})))
	}
	if cap(impl.nodes) != 0 {
		charge += ramCharge(uint64(cap(impl.nodes)) * uint64(unsafe.Sizeof([]ramNode{})))
	}
	return charge
}

func (impl *ramEngine) appendBytes(value []byte) ramSpan {
	if len(value) == 0 {
		return ramSpan{}
	}
	index := int(impl.tail) - 1
	if index < 0 || len(impl.chunks[index].data)-int(impl.chunks[index].used) < len(value) {
		index = -1
		for ordinal := range impl.chunks {
			if impl.chunks[ordinal].data == nil {
				index = ordinal
				break
			}
		}
		if index == -1 {
			impl.chunks = append(impl.chunks, ramChunk{})
			index = len(impl.chunks) - 1
		}
		impl.chunks[index] = ramChunk{data: make([]byte, max(ramChunkBytes, len(value)))}
	}
	impl.tail = uint32(index) + 1
	chunk := &impl.chunks[index]
	span := ramSpan{uint32(index), chunk.used, uint32(len(value))}
	copy(chunk.data[chunk.used:], value)
	chunk.used += uint32(len(value))
	chunk.live += uint64(len(value))
	return span
}

func (impl *ramEngine) get(key []byte) ([]byte, bool, error) {
	impl.mutex.RLock()
	defer impl.mutex.RUnlock()
	index := impl.find(key)
	if index == 0 {
		return nil, false, nil
	}
	value := impl.data(impl.node(index).value)
	charge := ramCharge(uint64(len(value)))
	if err := impl.budget.reserve(charge); err != nil {
		return nil, false, err
	}
	defer impl.budget.release(charge)
	return bytes.Clone(value), true, nil
}

func (impl *ramEngine) scan(ctx context.Context, prefix, after []byte, limit int) ([]Entry, error) {
	impl.mutex.RLock()
	defer impl.mutex.RUnlock()
	var indexes [1024]uint32
	count, size := 0, 0
	var walk func(uint32) error
	walk = func(index uint32) error {
		if index == 0 || count == limit {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		node := impl.node(index)
		key := impl.data(node.key)
		if bytes.Compare(key, prefix) >= 0 && (len(after) == 0 || bytes.Compare(key, after) > 0) {
			if err := walk(node.left); err != nil {
				return err
			}
			if count == limit {
				return nil
			}
			if bytes.HasPrefix(key, prefix) {
				size += len(key) + int(node.value.size) + 16
				if size > MaxScanBytes {
					return ErrBatchLimit
				}
				indexes[count] = index
				count++
			}
		} else {
			return walk(node.right)
		}
		if len(prefix) > 0 && bytes.Compare(key, prefix) > 0 && !bytes.HasPrefix(key, prefix) {
			return nil
		}
		return walk(node.right)
	}
	if err := walk(impl.root); err != nil {
		return nil, err
	}
	charge := ramCharge(uint64(size)) + ramCharge(uint64(count)*uint64(unsafe.Sizeof(Entry{})))
	if err := impl.budget.reserve(charge); err != nil {
		return nil, err
	}
	defer impl.budget.release(charge)
	rows := make([]Entry, count)
	for ordinal := range count {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		node := impl.node(indexes[ordinal])
		rows[ordinal] = Entry{bytes.Clone(impl.data(node.key)), bytes.Clone(impl.data(node.value))}
	}
	return rows, nil
}
func (impl *ramEngine) close() error {
	impl.mutex.Lock()
	defer impl.mutex.Unlock()
	impl.chunks = nil
	impl.nodes = nil
	impl.count = 0
	impl.root = 0
	impl.tail = 0
	impl.budget.release(impl.charged)
	impl.charged = 0
	return nil
}

type RAMSnapshot struct {
	Entries                  uint64
	ArenaCapacityBytes       uint64
	IndexCapacityBytes       uint64
	MetadataCapacityBytes    uint64
	AccountedRetainedBytes   uint64
	ArenaChunks, IndexChunks uint64
	Budget                   BudgetSnapshot
}

func (store *Store) RAMSnapshot() (RAMSnapshot, bool) {
	impl, ok := store.engine.(*ramEngine)
	if !ok {
		return RAMSnapshot{}, false
	}
	impl.mutex.RLock()
	defer impl.mutex.RUnlock()
	result := RAMSnapshot{Entries: uint64(impl.count), AccountedRetainedBytes: impl.charged, Budget: impl.budget.Snapshot()}
	for _, chunk := range impl.chunks {
		if chunk.data != nil {
			result.ArenaCapacityBytes += uint64(len(chunk.data))
			result.ArenaChunks++
		}
	}
	result.IndexChunks = uint64(len(impl.nodes))
	result.IndexCapacityBytes = uint64(len(impl.nodes)) * ramNodeCount * uint64(unsafe.Sizeof(ramNode{}))
	result.MetadataCapacityBytes = uint64(cap(impl.chunks))*uint64(unsafe.Sizeof(ramChunk{})) + uint64(cap(impl.nodes))*uint64(unsafe.Sizeof([]ramNode{}))
	return result, true
}
