package maintenance

import (
	"context"
	"fmt"
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
