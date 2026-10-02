package maintenance

import (
	"context"
	"errors"
	"fmt"
	"github.com/otuschhoff/vaultic/internal/workingkv"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestCheckKVSpoolSortsDuplicatesAcrossMergePasses(t *testing.T) {
	scratch, err := newCheckScratch(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer scratch.close()
	spool, err := newCheckKVSpool(context.Background(), scratch, 16, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer spool.close()
	for _, record := range []checkKVRecord{
		{key: []byte("b"), value: []byte("3"), sequence: 3},
		{key: []byte("a"), value: []byte("2"), sequence: 2},
		{key: []byte("a"), value: []byte("1"), sequence: 1},
		{key: []byte("c"), value: []byte("4"), sequence: 4},
	} {
		if err := spool.add(record.key, record.value, record.sequence); err != nil {
			t.Fatal(err)
		}
	}
	iterator, err := spool.iterator()
	if err != nil {
		t.Fatal(err)
	}
	defer iterator.close()
	for index, want := range []string{"a:1:1", "a:2:2", "b:3:3", "c:4:4"} {
		record, found, err := iterator.next()
		if err != nil || !found {
			t.Fatalf("record %d: found=%t err=%v", index, found, err)
		}
		got := fmt.Sprintf("%s:%s:%d", record.key, record.value, record.sequence)
		if got != want {
			t.Fatalf("record %d = %q, want %q", index, got, want)
		}
	}
	if _, found, err := iterator.next(); err != nil || found {
		t.Fatalf("unexpected trailing record: found=%t err=%v", found, err)
	}
	if _, merges := scratch.stats(); merges == 0 {
		t.Fatal("tiny fan-in did not produce a merge pass")
	}
}

func TestCheckKVSpoolKeepsFittingRecordsInMemory(t *testing.T) {
	scratch, err := newCheckScratch(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer scratch.close()
	spool, err := newCheckKVSpool(context.Background(), scratch, 1<<10, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer spool.close()
	for _, record := range []checkKVRecord{
		{key: []byte("b"), value: []byte("2"), sequence: 2},
		{key: []byte("a"), value: []byte("1"), sequence: 1},
	} {
		if err := spool.add(record.key, record.value, record.sequence); err != nil {
			t.Fatal(err)
		}
	}
	iterator, err := spool.iterator()
	if err != nil {
		t.Fatal(err)
	}
	defer iterator.close()
	if record, found, err := iterator.next(); err != nil || !found || string(record.key) != "a" {
		t.Fatalf("first record = %+v, found=%t, err=%v", record, found, err)
	}
	if peak, _ := scratch.stats(); peak != 0 {
		t.Fatalf("fitting key/value spool used %d bytes of disk scratch", peak)
	}
}

func TestCheckKVSpoolRejectsOversizedRecord(t *testing.T) {
	scratch, err := newCheckScratch(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer scratch.close()
	spool, err := newCheckKVSpool(context.Background(), scratch, 16, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer spool.close()
	if err := spool.add([]byte("too-large"), []byte("value"), 1); err == nil {
		t.Fatal("oversized key/value record was accepted")
	}
}

func TestM3RAMSpillRejection(t *testing.T) {
	root := t.TempDir()
	policy, _ := workingkv.NewPolicy(workingkv.ModeRAM, 1<<20, filepath.Join(root, "absent"))
	ctx := workingkv.WithPolicy(t.Context(), policy)
	scratch, err := newCheckScratchWithScenario(ctx, filepath.Join(root, "absent"), 1<<20, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer scratch.close()
	kv, err := newCheckKVSpool(ctx, scratch, 16, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer kv.close()
	if err := kv.add([]byte("a"), []byte("1"), 1); err != nil {
		t.Fatal(err)
	}
	if err := kv.add([]byte("b"), []byte("2"), 2); !errors.Is(err, workingkv.ErrWorkingMemoryLimitExceeded) {
		t.Fatal("KV spool silently spilled", err)
	}
	locations, err := newLocationMultisetSpool(ctx, scratch, locationTupleMemorySize*2, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer locations.close()
	for _, ordinal := range []byte{1, 1} {
		if err := locations.add(testLocation(ordinal)); err != nil {
			t.Fatal(err)
		}
	}
	if err := locations.add(testLocation(2)); !errors.Is(err, workingkv.ErrWorkingMemoryLimitExceeded) {
		t.Fatal("location spool silently spilled", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 0 {
		t.Fatal("forced RAM touched absent scratch", err)
	}
}

func TestM3SorterParity(t *testing.T) {
	baseline := []locationTuple{testLocation(1), testLocation(1), testLocation(2), testLocation(3), testLocation(3), testLocation(3), testLocation(7)}
	var baselineKV []string
	for _, ordinal := range []uint64{1, 1, 2, 3} {
		baselineKV = append(baselineKV, fmt.Sprintf("%x:%x:%d", []byte("key\x00"), []byte{byte(ordinal)}, ordinal))
	}
	for _, mode := range []workingkv.Mode{workingkv.ModeRAM, workingkv.ModeKV} {
		t.Run(string(mode), func(t *testing.T) {
			root := t.TempDir()
			policy, _ := workingkv.NewPolicy(mode, 2<<20, root)
			ctx := workingkv.WithPolicy(t.Context(), policy)
			scratch, err := newCheckScratchWithScenario(ctx, root, 2<<20, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer scratch.close()
			memory := locationTupleMemorySize * 16
			kvMemory := uint64(1024)
			if mode == workingkv.ModeKV {
				memory = locationTupleMemorySize * 2
				kvMemory = 32
			}
			spool, err := newLocationMultisetSpool(ctx, scratch, memory, 2)
			if err != nil {
				t.Fatal(err)
			}
			defer spool.close()
			for _, ordinal := range []byte{3, 1, 3, 2, 7, 1, 3} {
				if err := spool.add(testLocation(ordinal)); err != nil {
					t.Fatal(err)
				}
			}
			iterator, err := spool.iterator()
			if err != nil {
				t.Fatal(err)
			}
			var tuples []locationTuple
			for {
				tuple, found, err := iterator.next()
				if err != nil {
					t.Fatal(err)
				}
				if !found {
					break
				}
				tuples = append(tuples, tuple)
			}
			if err := iterator.close(); err != nil {
				t.Fatal(err)
			}
			kv, err := newCheckKVSpool(ctx, scratch, kvMemory, 2)
			if err != nil {
				t.Fatal(err)
			}
			defer kv.close()
			for _, ordinal := range []uint64{3, 1, 2, 1} {
				if err := kv.add([]byte("key\x00"), []byte{byte(ordinal)}, ordinal); err != nil {
					t.Fatal(err)
				}
			}
			kvIterator, err := kv.iterator()
			if err != nil {
				t.Fatal(err)
			}
			var values []string
			for {
				value, found, err := kvIterator.next()
				if err != nil {
					t.Fatal(err)
				}
				if !found {
					break
				}
				values = append(values, fmt.Sprintf("%x:%x:%d", value.key, value.value, value.sequence))
			}
			if err := kvIterator.close(); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(tuples, baseline) || !reflect.DeepEqual(values, baselineKV) {
				t.Fatal("sorted multiset/sequence parity differs")
			}
			peak, _ := scratch.stats()
			if mode == workingkv.ModeRAM && peak != 0 || mode == workingkv.ModeKV && peak == 0 {
				t.Fatal("sorter mode contract", peak)
			}
			if err := spool.close(); err != nil {
				t.Fatal(err)
			}
			if err := kv.close(); err != nil {
				t.Fatal(err)
			}
			if policy.Budget().Used != 0 {
				t.Fatal("sort reservation leaked")
			}
		})
	}
}

func TestM3SorterReservationTransfer(t *testing.T) {
	policy, _ := workingkv.NewPolicy(workingkv.ModeRAM, 1<<20, "")
	ctx := workingkv.WithPolicy(t.Context(), policy)
	scratch, _ := newCheckScratchWithScenario(ctx, "", 1<<20, nil)
	defer scratch.close()
	source, _ := newLocationSpool(ctx, scratch, locationTupleMemorySize*4, 2)
	target, _ := newLocationSpool(ctx, scratch, locationTupleMemorySize*4, 2)
	defer target.close()
	if err := source.add(testLocation(1)); err != nil {
		t.Fatal(err)
	}
	before := policy.Budget().Used
	if err := target.adopt(source); err != nil {
		t.Fatal(err)
	}
	if err := source.close(); err != nil {
		t.Fatal(err)
	}
	if policy.Budget().Used != before {
		t.Fatal("adoption released live capacity")
	}
	if err := target.close(); err != nil {
		t.Fatal(err)
	}
	if policy.Budget().Used != 0 {
		t.Fatal("adoption double charged/leaked")
	}
}
