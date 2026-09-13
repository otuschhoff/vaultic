package vaultic

import (
	"context"
	"testing"
)

func TestReadLogicalFileRange(t *testing.T) {
	ids := []ID{{1}, {2}, {3}}
	cum := []uint64{0, 3, 7, 9}
	blobs := map[byte][]byte{
		1: []byte("abc"),
		2: []byte("defg"),
		3: []byte("hi"),
	}
	buf := make([]byte, 5)
	read, err := ReadLogicalFileRange(context.Background(), ids, cum, 2, buf, func(_ context.Context, _ int, id ID) ([]byte, error) {
		return blobs[id[0]], nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if read != 5 {
		t.Fatalf("read bytes = %d, want 5", read)
	}
	if got := string(buf[:read]); got != "cdefg" {
		t.Fatalf("data = %q, want cdefg", got)
	}
}
