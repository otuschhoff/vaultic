package workingkv

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/debug"
	"sync"
	"testing"

	"github.com/otuschhoff/vaultic/internal/telemetry"
)

func openConformanceStore(t *testing.T, backend string) (*Store, error) {
	t.Helper()
	if backend == "ram" {
		budget, _ := NewBudget(128 << 20)
		return OpenRAM(budget)
	}
	return Open(backend, filepath.Join(t.TempDir(), "store"))
}

func TestRAMConformanceAndBudget(t *testing.T) {
	budget, _ := NewBudget(2 << 20)
	store, err := OpenRAM(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	entries := []Entry{{[]byte("p\x00/2"), []byte("two")}, {[]byte("p\x00/1"), nil}, {[]byte("q"), []byte("other")}}
	if err := store.Put(t.Context(), entries); err != nil {
		t.Fatal(err)
	}
	rows, err := store.Scan(t.Context(), []byte("p\x00/"), nil, 1)
	if err != nil || len(rows) != 1 || !bytes.Equal(rows[0].Key, entries[1].Key) {
		t.Fatal("prefix order", err)
	}
	rows, err = store.Scan(t.Context(), []byte("p\x00/"), rows[0].Key, 1)
	if err != nil || len(rows) != 1 || string(rows[0].Value) != "two" {
		t.Fatal("pagination", err)
	}
	if err := store.Put(t.Context(), []Entry{{entries[0].Key, []byte("new")}}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if budget.Snapshot().Used != 0 {
		t.Fatal("retained reservation leaked", budget.Snapshot())
	}
	tiny, _ := NewBudget(ramBaseCharge)
	limited, err := OpenRAM(tiny)
	if err != nil {
		t.Fatal(err)
	}
	defer limited.Close()
	err = limited.Put(t.Context(), entries)
	var detail *MemoryLimitError
	if !errors.Is(err, ErrWorkingMemoryLimitExceeded) || !errors.As(err, &detail) {
		t.Fatal("typed exhaustion", err)
	}
	if _, found, err := limited.Get(t.Context(), entries[0].Key); err != nil || found {
		t.Fatal("partial exhausted batch")
	}
	if tiny.Snapshot().Used != ramBaseCharge {
		t.Fatal("failed admission changed usage")
	}
}

func TestRAMReplacementAccounting(t *testing.T) {
	budget, _ := NewBudget(4 << 20)
	store, _ := OpenRAM(budget)
	defer store.Close()
	key := []byte("same")
	if err := store.Put(t.Context(), []Entry{{key, bytes.Repeat([]byte{1}, 128<<10)}}); err != nil {
		t.Fatal(err)
	}
	initial := budget.Snapshot().Used
	for range 100 {
		if err := store.Put(t.Context(), []Entry{{key, []byte("short")}, {key, bytes.Repeat([]byte{2}, 128<<10)}}); err != nil {
			t.Fatal(err)
		}
	}
	if budget.Snapshot().Used != initial {
		t.Fatal("in-place overwrite leaked reservation", budget.Snapshot())
	}
	if err := store.Put(t.Context(), []Entry{{key, bytes.Repeat([]byte{3}, 512<<10)}}); err != nil {
		t.Fatal(err)
	}
	impl := store.engine.(*ramEngine)
	if budget.Snapshot().Used != impl.retainedCharge() {
		t.Fatal("reservation does not match retained capacities")
	}
	value, _, err := store.Get(t.Context(), key)
	if err != nil || len(value) != 512<<10 || value[0] != 3 {
		t.Fatal("replacement mismatch", err)
	}
	for ordinal := range 100 {
		if err := store.Put(t.Context(), []Entry{{[]byte{byte(ordinal), 0xff}, []byte("hole reuse")}}); err != nil {
			t.Fatal(err)
		}
	}
	if budget.Snapshot().Used != impl.retainedCharge() {
		t.Fatal("reused-hole accounting mismatch")
	}
}

func TestRAMAdmissionBoundariesAndSharedRaces(t *testing.T) {
	if _, err := NewBudget(0); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := OpenRAM(nil); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	budget, _ := NewBudget(4 * ramBaseCharge)
	var group sync.WaitGroup
	var mutex sync.Mutex
	var stores []*Store
	for range 32 {
		group.Go(func() {
			store, err := OpenRAM(budget)
			if err != nil {
				if !errors.Is(err, ErrWorkingMemoryLimitExceeded) {
					t.Error(err)
				}
				return
			}
			mutex.Lock()
			stores = append(stores, store)
			mutex.Unlock()
		})
	}
	group.Wait()
	if len(stores) != 4 || budget.Snapshot().Used != 4*ramBaseCharge {
		t.Fatal("concurrent admission overshot", budget.Snapshot(), len(stores))
	}
	for _, store := range stores {
		group.Go(func() {
			if err := store.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	group.Wait()
	if budget.Snapshot().Used != 0 {
		t.Fatal("shared close leaked", budget.Snapshot())
	}
	probe, _ := NewBudget(2 << 20)
	store, _ := OpenRAM(probe)
	entry := []Entry{{[]byte("boundary"), []byte("value")}}
	if err := store.Put(t.Context(), entry); err != nil {
		t.Fatal(err)
	}
	required := probe.Snapshot().Peak
	store.Close()
	for _, limit := range []uint64{required - 1, required} {
		budget, _ := NewBudget(limit)
		store, err := OpenRAM(budget)
		if err != nil {
			t.Fatal(err)
		}
		err = store.Put(t.Context(), entry)
		if limit < required && !errors.Is(err, ErrWorkingMemoryLimitExceeded) || limit == required && err != nil {
			t.Fatalf("boundary %d/%d: %v", limit, required, err)
		}
		if budget.Snapshot().Peak > limit {
			t.Fatal("limit overshot")
		}
		store.Close()
		if budget.Snapshot().Used != 0 {
			t.Fatal("boundary release leaked")
		}
	}
}

func TestRAMTreeOrderingAndPointerLayout(t *testing.T) {
	typeOf := reflect.TypeOf(ramNode{})
	var checkType func(reflect.Type)
	checkType = func(kind reflect.Type) {
		if kind.Kind() == reflect.Struct {
			for ordinal := range kind.NumField() {
				checkType(kind.Field(ordinal).Type)
			}
			return
		}
		switch kind.Kind() {
		case reflect.Uint32, reflect.Uint64:
		default:
			t.Fatalf("retained node contains pointer/scanned kind %s", kind)
		}
	}
	checkType(typeOf)
	budget, _ := NewBudget(16 << 20)
	store, _ := OpenRAM(budget)
	defer store.Close()
	for first := 0; first < 8192; first += 128 {
		entries := make([]Entry, 128)
		for ordinal := range entries {
			key := fmt.Appendf(nil, "%08d", 8191-first-ordinal)
			entries[ordinal] = Entry{key, bytes.Clone(key)}
		}
		if err := store.Put(t.Context(), entries); err != nil {
			t.Fatal(err)
		}
	}
	impl := store.engine.(*ramEngine)
	var validate func(uint32) uint32
	validate = func(index uint32) uint32 {
		if index == 0 {
			return 0
		}
		node := impl.node(index)
		left, right := validate(node.left), validate(node.right)
		if int(left)-int(right) > 1 || int(right)-int(left) > 1 || node.height != 1+max(left, right) {
			t.Fatal("unbalanced index")
		}
		return node.height
	}
	if validate(impl.root) > 20 {
		t.Fatal("linear index height")
	}
	var after []byte
	count := 0
	for {
		rows, err := store.Scan(t.Context(), nil, after, 37)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) == 0 {
			break
		}
		for _, row := range rows {
			if string(row.Key) != fmt.Sprintf("%08d", count) {
				t.Fatal("ordered scan mismatch", count, row.Key)
			}
			count++
		}
		after = rows[len(rows)-1].Key
	}
	if count != 8192 {
		t.Fatal("scan omitted entries", count)
	}
	state, _ := store.RAMSnapshot()
	if state.AccountedRetainedBytes != budget.Snapshot().Used || state.Entries != 8192 || state.IndexChunks != 32 {
		t.Fatal("capacity snapshot mismatch", state)
	}
}

func TestRAMEncodedConsumerReplay(t *testing.T) {
	for _, kind := range []string{"overlay", "markers", "directories"} {
		t.Run(kind, func(t *testing.T) {
			fixture := newReplayFixture(kind)
			budget, _ := NewBudget(64 << 20)
			store, err := OpenRAM(budget)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			for _, round := range fixture.rounds {
				for first := 0; first < len(round); {
					last, size := first, 0
					for last < len(round) && size+len(round[last].Key)+len(round[last].Value)+16 <= MaxBatchBytes {
						size += len(round[last].Key) + len(round[last].Value) + 16
						last++
					}
					if last == first {
						t.Fatal("oversized fixture")
					}
					if err := store.Put(t.Context(), round[first:last]); err != nil {
						t.Fatal(err)
					}
					first = last
				}
			}
			var after []byte
			var result []Entry
			for {
				rows, err := store.Scan(t.Context(), nil, after, 8)
				if err != nil {
					t.Fatal(err)
				}
				if len(rows) == 0 {
					break
				}
				result = append(result, rows...)
				after = rows[len(rows)-1].Key
			}
			if entriesDigest(result) != entriesDigest(fixture.final) {
				t.Fatal("RAM/KV fixture digest mismatch")
			}
			if kind == "overlay" {
				rows, err := store.Scan(t.Context(), fixture.final[0].Key[:32], nil, 8)
				if err != nil || len(rows) != 2 {
					t.Fatal("duplicate-location prefix mismatch", err)
				}
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			if budget.Snapshot().Used != 0 {
				t.Fatal("fixture release leaked")
			}
		})
	}
}

func TestRAMReadAdmissionAndCancelledReservations(t *testing.T) {
	budget, _ := NewBudget(2 << 20)
	store, _ := OpenRAM(budget)
	defer store.Close()
	entry := Entry{[]byte("wide"), bytes.Repeat([]byte{1}, 512<<10)}
	if err := store.Put(t.Context(), []Entry{entry}); err != nil {
		t.Fatal(err)
	}
	remaining := budget.Snapshot().Limit - budget.Snapshot().Used
	if err := budget.reserve(remaining); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Get(t.Context(), entry.Key); !errors.Is(err, ErrWorkingMemoryLimitExceeded) {
		t.Fatal("copy allocated without admission", err)
	}
	if rows, err := store.Scan(t.Context(), nil, nil, 1); !errors.Is(err, ErrWorkingMemoryLimitExceeded) || rows != nil {
		t.Fatal("page allocated without admission", err)
	}
	budget.release(remaining)
	before := budget.Snapshot().Used
	parent, cancel := context.WithCancel(t.Context())
	defer cancel()
	ctx := &cancelDuringCopy{Context: parent, cancel: cancel}
	if err := store.Put(ctx, []Entry{{[]byte("cancelled"), []byte("new")}}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if budget.Snapshot().Used != before {
		t.Fatal("cancelled reservation leaked")
	}
	if err := store.Put(t.Context(), []Entry{{[]byte("too-wide"), make([]byte, MaxBatchBytes)}}); !errors.Is(err, ErrBatchLimit) {
		t.Fatal(err)
	}
}

func BenchmarkRAMScaling(b *testing.B) {
	for _, count := range []int{4096, 65536, 262144} {
		b.Run(fmt.Sprintf("entries-%d", count), func(b *testing.B) {
			if b.N != 1 {
				b.Fatal("requires -benchtime=1x")
			}
			runtime.GC()
			before := telemetry.ReadWorkingRuntime()
			budget, _ := NewBudget(256 << 20)
			store, err := OpenRAM(budget)
			if err != nil {
				b.Fatal(err)
			}
			defer store.Close()
			keys := make([]byte, 128*8)
			value := make([]byte, 64)
			entries := make([]Entry, 128)
			for first := 0; first < count; first += 128 {
				for ordinal := range entries {
					key := keys[ordinal*8 : (ordinal+1)*8]
					binary.BigEndian.PutUint64(key, uint64(first+ordinal))
					entries[ordinal] = Entry{key, value}
				}
				if err := store.Put(b.Context(), entries); err != nil {
					b.Fatal(err)
				}
			}
			state, _ := store.RAMSnapshot()
			runtime.GC()
			after := telemetry.ReadWorkingRuntime()
			if state.Entries != uint64(count) || state.AccountedRetainedBytes != budget.Snapshot().Used {
				b.Fatal("scaling capacity mismatch")
			}
			if after.ScannableHeapBytes > before.ScannableHeapBytes+(1<<20)+state.MetadataCapacityBytes*4 {
				b.Fatal("retained heap pointer scanning exceeded layout bound")
			}
			if after.HeapObjects > before.HeapObjects+state.ArenaChunks+state.IndexChunks+2048 {
				b.Fatal("per-entry retained heap objects")
			}
			if after.HeapBytes > before.HeapBytes+state.AccountedRetainedBytes+(1<<20) {
				b.Fatal("measured heap exceeds conservative retained accounting")
			}
			if err := store.Close(); err != nil {
				b.Fatal(err)
			}
			if budget.Snapshot().Used != 0 {
				b.Fatal("scaling release leak")
			}
			debug.FreeOSMemory()
			released := telemetry.ReadWorkingRuntime()
			record := struct {
				Entries                 int         `json:"entries"`
				State                   RAMSnapshot `json:"state"`
				Before, After, Released telemetry.WorkingRuntimeSnapshot
			}{count, state, before, after, released}
			encoded, err := json.Marshal(record)
			if err != nil {
				b.Fatal(err)
			}
			b.Logf("phase35_m2=%s", encoded)
		})
	}
}

func TestRAMFailedGrowthPreservesState(t *testing.T) {
	budget, _ := NewBudget(2 << 20)
	store, _ := OpenRAM(budget)
	defer store.Close()
	if err := store.Put(t.Context(), []Entry{{[]byte("seed"), []byte("original")}}); err != nil {
		t.Fatal(err)
	}
	before, _ := store.RAMSnapshot()
	held := budget.Snapshot().Limit - budget.Snapshot().Used - 4096
	if err := budget.reserve(held); err != nil {
		t.Fatal(err)
	}
	err := store.Put(t.Context(), []Entry{{[]byte("seed"), []byte("changed")}, {[]byte("new"), make([]byte, 128<<10)}})
	var detail *MemoryLimitError
	if !errors.As(err, &detail) || !errors.Is(err, ErrWorkingMemoryLimitExceeded) || errors.Is(err, ErrBackendIO) {
		t.Fatal("incorrect growth failure", err)
	}
	budget.release(held)
	after, _ := store.RAMSnapshot()
	if before.AccountedRetainedBytes != after.AccountedRetainedBytes || before.Entries != after.Entries || before.ArenaCapacityBytes != after.ArenaCapacityBytes {
		t.Fatal("failed batch mutated capacity")
	}
	value, found, err := store.Get(t.Context(), []byte("seed"))
	if err != nil || !found || string(value) != "original" {
		t.Fatal("failed batch mutated seed", err)
	}
	if _, found, err := store.Get(t.Context(), []byte("new")); err != nil || found {
		t.Fatal("failed batch inserted new key", err)
	}
}

func TestRAMSharedWriteReservations(t *testing.T) {
	probe, _ := NewBudget(2 << 20)
	store, _ := OpenRAM(probe)
	entry := []Entry{{[]byte("key"), []byte("value")}}
	if err := store.Put(t.Context(), entry); err != nil {
		t.Fatal(err)
	}
	limit := probe.Snapshot().Peak + ramBaseCharge
	store.Close()
	for range 32 {
		budget, _ := NewBudget(limit)
		left, _ := OpenRAM(budget)
		right, _ := OpenRAM(budget)
		start := make(chan struct{})
		results := make(chan error, 2)
		var group sync.WaitGroup
		for _, store := range []*Store{left, right} {
			group.Go(func() { <-start; results <- store.Put(t.Context(), entry) })
		}
		close(start)
		group.Wait()
		close(results)
		success, denied := 0, 0
		for err := range results {
			if err == nil {
				success++
			} else if errors.Is(err, ErrWorkingMemoryLimitExceeded) {
				denied++
			} else {
				t.Fatal(err)
			}
		}
		if success > 1 || denied < 1 || success+denied != 2 || budget.Snapshot().Peak > limit {
			t.Fatal("shared write reservation race", success, denied, budget.Snapshot())
		}
		if success == 0 {
			if err := left.Put(t.Context(), entry); err != nil {
				t.Fatal("released planning reservations prevented retry", err)
			}
			if err := right.Put(t.Context(), entry); !errors.Is(err, ErrWorkingMemoryLimitExceeded) {
				t.Fatal("retry overspent shared capacity", err)
			}
		}
		left.Close()
		right.Close()
		if budget.Snapshot().Used != 0 {
			t.Fatal("shared writer release leaked")
		}
	}
}

func TestRAMCancelledQueuedRead(t *testing.T) {
	budget, _ := NewBudget(2 << 20)
	store, _ := OpenRAM(budget)
	defer store.Close()
	if err := store.Put(t.Context(), []Entry{{[]byte("key"), []byte("value")}}); err != nil {
		t.Fatal(err)
	}
	impl := store.engine.(*ramEngine)
	for _, scan := range []bool{false, true} {
		ctx, cancel := context.WithCancel(t.Context())
		impl.mutex.Lock()
		started := make(chan struct{})
		result := make(chan error, 1)
		store.engine = &ramReadSignalEngine{engine: impl, entered: started}
		go func() {
			if scan {
				rows, err := store.Scan(ctx, nil, nil, 1)
				if rows != nil {
					t.Error("cancelled scan returned rows")
				}
				result <- err
			} else {
				value, found, err := store.Get(ctx, []byte("key"))
				if value != nil || found {
					t.Error("cancelled get returned value")
				}
				result <- err
			}
		}()
		<-started
		cancel()
		impl.mutex.Unlock()
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	}
}

type ramReadSignalEngine struct {
	engine
	entered chan struct{}
}

func (impl *ramReadSignalEngine) get(key []byte) ([]byte, bool, error) {
	close(impl.entered)
	return impl.engine.get(key)
}

func (impl *ramReadSignalEngine) scan(ctx context.Context, prefix, after []byte, limit int) ([]Entry, error) {
	close(impl.entered)
	return impl.engine.scan(ctx, prefix, after, limit)
}
