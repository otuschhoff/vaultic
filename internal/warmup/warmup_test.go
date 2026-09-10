package warmup

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/backend"
	rtest "github.com/otuschhoff/vaultic/internal/test"
)

func handle(name string) backend.Handle {
	return backend.Handle{Type: backend.PackFile, Name: name}
}

func pathOf(h backend.Handle) string { return "data/" + h.Name[:2] + "/" + h.Name }

func TestWarmupDisabled(t *testing.T) {
	r := New(Options{}, nil, nil)
	rtest.Assert(t, !r.Enabled(), "warmup should be disabled without a command")
	rtest.OK(t, r.Warmup(context.TODO(), []backend.Handle{handle("aa")}, pathOf))
}

func TestWarmupBatchIDs(t *testing.T) {
	out := filepath.Join(t.TempDir(), "calls.txt")
	opts := Options{Command: "%ids", Batch: 2}

	r := New(opts, nil, nil)
	r.command = warmupHelperCommand(out)
	handles := []backend.Handle{handle("aaaa"), handle("bbbb"), handle("cccc"), handle("dddd"), handle("eeee")}
	rtest.OK(t, r.Warmup(context.TODO(), handles, pathOf))

	// 5 packs, batch 2 -> 3 invocations (2+2+1)
	data := readLines(t, out)
	rtest.Equals(t, 3, len(data))
	rtest.Equals(t, "aaaa bbbb", data[0])
	rtest.Equals(t, "cccc dddd", data[1])
	rtest.Equals(t, "eeee", data[2])
}

func TestWarmupParallelPerID(t *testing.T) {
	out := filepath.Join(t.TempDir(), "calls.txt")
	opts := Options{Command: "%id", Batch: 2}
	r := New(opts, nil, nil)
	r.command = warmupHelperCommand(out)
	handles := []backend.Handle{handle("aa"), handle("bb"), handle("cc")}
	rtest.OK(t, r.Warmup(context.TODO(), handles, pathOf))

	lines := readLines(t, out)
	rtest.Equals(t, 3, len(lines))
	// all ids present (order not guaranteed for parallel runs)
	joined := strings.Join(lines, " ")
	for _, id := range []string{"aa", "bb", "cc"} {
		rtest.Assert(t, strings.Contains(joined, id), "missing id %q in %v", id, lines)
	}
}

func TestWarmupCommandHelper(t *testing.T) {
	if len(os.Args) < 3 || os.Args[1] != "-test.run=TestWarmupCommandHelper" || os.Args[2] != "--" {
		return
	}
	if len(os.Args) != 5 {
		t.Fatal("warmup helper arguments are incomplete")
	}
	file, err := os.OpenFile(os.Args[3], os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := fmt.Fprintln(file, os.Args[4]); err != nil {
		t.Fatal(err)
	}
}

func warmupHelperCommand(output string) func(context.Context, string) *exec.Cmd {
	return func(ctx context.Context, command string) *exec.Cmd {
		return exec.CommandContext(ctx, os.Args[0], "-test.run=TestWarmupCommandHelper", "--", output, command)
	}
}

func TestWarmupProgressProtocol(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("progress fixture uses POSIX shell quoting and command separators")
	}
	// use %ids so both packs are in ONE invocation; the command reports
	// pack-progress within that invocation, interleaved with plain text
	cmd := `echo 'starting'; echo '{"type":"pack-progress","warm":1}'; echo '{"type":"pack-progress","warm":2}'`
	var mu sync.Mutex
	var progress []int
	var logs []string
	r := New(Options{Command: cmd + " # %ids", Batch: 2},
		func(warm, total int) { mu.Lock(); progress = append(progress, warm); mu.Unlock() },
		func(msg string) { mu.Lock(); logs = append(logs, msg); mu.Unlock() },
	)
	rtest.OK(t, r.Warmup(context.TODO(), []backend.Handle{handle("aa"), handle("bb")}, pathOf))

	// progress is per-invocation and monotonic: 1 then 2
	rtest.Equals(t, []int{1, 2}, progress)
	rtest.Equals(t, []string{"[warmup] starting"}, logs)
}

func TestWarmupFailurePropagates(t *testing.T) {
	r := New(Options{Command: "exit 3"}, nil, nil)
	err := r.Warmup(context.TODO(), []backend.Handle{handle("aa")}, pathOf)
	rtest.Assert(t, err != nil, "expected error from failing warm-up command")
}

func TestWarmupWait(t *testing.T) {
	start := time.Now()
	command := "true"
	if runtime.GOOS == "windows" {
		command = "ver > nul"
	}
	r := New(Options{Command: command, Wait: 50 * time.Millisecond}, nil, nil)
	rtest.OK(t, r.Warmup(context.TODO(), []backend.Handle{handle("aa")}, pathOf))
	rtest.Assert(t, time.Since(start) >= 50*time.Millisecond, "warm-up wait was not honored")
}

func readLines(t *testing.T, path string) []string {
	t.Helper()
	data, err := os.ReadFile(path)
	rtest.OK(t, err)
	var out []string
	for _, l := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if l != "" {
			out = append(out, l)
		}
	}
	return out
}
