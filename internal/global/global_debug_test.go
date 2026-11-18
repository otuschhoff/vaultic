//go:build debug || profile

package global

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime/trace"
	"testing"
)

func TestBoundedRuntimeTrace(t *testing.T) {
	for _, limit := range []uint64{1, 8 << 20} {
		directory := t.TempDir()
		var stderr bytes.Buffer
		bounded, err := startBoundedRuntimeTrace(directory, limit, &stderr)
		if err != nil {
			t.Fatal(err)
		}
		trace.Log(t.Context(), "vaultic-test", "bounded")
		bounded.Stop()
		info, err := os.Stat(filepath.Join(directory, "trace.out"))
		if err != nil || uint64(info.Size()) > limit || info.Mode().Perm() != 0600 {
			t.Fatalf("unsafe trace: %v, %v", info, err)
		}
		receipt, err := os.ReadFile(filepath.Join(directory, "trace.complete"))
		if limit == 1 {
			if !os.IsNotExist(err) || !bytes.Contains(stderr.Bytes(), []byte("exceeded its byte limit")) {
				t.Fatalf("overflow was not rejected: %v, %s", err, stderr.String())
			}
		} else if err != nil || string(receipt) != fmt.Sprintln(info.Size()) || info.Size() == 0 || stderr.Len() != 0 {
			t.Fatalf("trace was not accepted: %q, %v, %s", receipt, err, stderr.String())
		}
		if _, err := startBoundedRuntimeTrace(directory, limit, &stderr); err == nil {
			t.Fatal("existing trace was accepted for overwrite")
		}
	}
}
