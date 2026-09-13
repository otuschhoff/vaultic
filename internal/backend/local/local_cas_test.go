//go:build !windows

package local_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/backend/local"
	rtest "github.com/otuschhoff/vaultic/internal/test"
	"golang.org/x/sys/unix"
)

type casResult struct {
	Current string `json:"current"`
	Swapped bool   `json:"swapped"`
}

type casProcessResult struct {
	Result casResult
	Err    error
}

func TestLocalCompareAndSwapInterprocessCreateRace(t *testing.T) {
	dir := rtest.TempDir(t)
	cfg := local.Config{Path: dir, Connections: 2}
	be, err := local.Create(context.Background(), cfg, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	_ = be.Close()

	handle := backend.Handle{Type: backend.StagingFile, Name: "coordination/policy.json"}
	procA := runCASSubprocess(t, dir, handle.Name, "value-a")
	procB := runCASSubprocess(t, dir, handle.Name, "value-b")

	outcomeA := <-procA
	outcomeB := <-procB
	if outcomeA.Err != nil {
		t.Fatal(outcomeA.Err)
	}
	if outcomeB.Err != nil {
		t.Fatal(outcomeB.Err)
	}
	resultA := outcomeA.Result
	resultB := outcomeB.Result

	swaps := 0
	if resultA.Swapped {
		swaps++
	}
	if resultB.Swapped {
		swaps++
	}
	if swaps != 1 {
		t.Fatalf("swapped count = %d, want 1 (a=%+v b=%+v)", swaps, resultA, resultB)
	}

	winner := "value-a"
	loser := resultB
	if resultB.Swapped {
		winner = "value-b"
		loser = resultA
	}
	if loser.Current != winner {
		t.Fatalf("loser observed current = %q, want %q", loser.Current, winner)
	}
}

func TestLocalCompareAndSwapRespectsContextDeadline(t *testing.T) {
	dir := rtest.TempDir(t)
	cfg := local.Config{Path: dir, Connections: 2}
	be, err := local.Create(context.Background(), cfg, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	defer be.Close()

	writer := backend.AsCapability[backend.ConditionalWriter](be)
	if writer == nil {
		t.Fatal("local backend does not expose conditional writer")
	}

	handle := backend.Handle{Type: backend.StagingFile, Name: "coordination/quota.json"}
	ctx, cancel := context.WithTimeout(t.Context(), time.Nanosecond)
	defer cancel()
	time.Sleep(2 * time.Millisecond)
	_, _, err = writer.CompareAndSwap(ctx, handle, nil, []byte("payload"))
	if err == nil {
		t.Fatal("CompareAndSwap unexpectedly succeeded with expired context")
	}
}

func TestLocalMutationsParticipateInCompareAndSwapLock(t *testing.T) {
	dir := rtest.TempDir(t)
	be, err := local.Create(context.Background(), local.Config{Path: dir, Connections: 2}, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	defer be.Close()
	handle := backend.Handle{Type: backend.StagingFile, Name: "coordination/quota.json"}
	localBackend := be
	if err := os.MkdirAll(filepath.Dir(localBackend.Filename(handle)), 0700); err != nil {
		t.Fatal(err)
	}
	lockPath := filepath.Join(dir, ".vaultic-cas.lock")
	if err := os.MkdirAll(filepath.Dir(lockPath), 0700); err != nil {
		t.Fatal(err)
	}
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer lockFile.Close()
	if err := unix.Flock(int(lockFile.Fd()), unix.LOCK_EX); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = unix.Flock(int(lockFile.Fd()), unix.LOCK_UN)
	}()

	for _, operation := range []struct {
		name string
		run  func(context.Context) error
	}{
		{name: "save", run: func(ctx context.Context) error {
			return be.Save(ctx, handle, backend.NewByteReader([]byte("new"), be.Hasher()))
		}},
		{name: "remove", run: func(ctx context.Context) error { return be.Remove(ctx, handle) }},
	} {
		t.Run(operation.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
			defer cancel()
			if err := operation.run(ctx); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("mutation error = %v, want context deadline exceeded", err)
			}
		})
	}
}

func runCASSubprocess(t *testing.T, repoPath, handleName, payload string) <-chan casProcessResult {
	t.Helper()
	results := make(chan casProcessResult, 1)
	go func() {
		command := exec.Command(os.Args[0], "-test.run", "^TestLocalCompareAndSwapSubprocessHelper$")
		command.Env = append(
			os.Environ(),
			"VAULTIC_LOCAL_CAS_HELPER=1",
			"VAULTIC_LOCAL_CAS_REPO="+repoPath,
			"VAULTIC_LOCAL_CAS_HANDLE="+handleName,
			"VAULTIC_LOCAL_CAS_PAYLOAD="+payload,
		)
		output, err := command.CombinedOutput()
		if err != nil {
			results <- casProcessResult{Err: fmt.Errorf("CAS helper failed: %w\n%s", err, output)}
			return
		}
		jsonLine := extractJSONLine(output)
		if len(jsonLine) == 0 {
			results <- casProcessResult{Err: fmt.Errorf("CAS helper returned no JSON output\n%s", output)}
			return
		}
		var result casResult
		if err := json.Unmarshal(jsonLine, &result); err != nil {
			results <- casProcessResult{Err: fmt.Errorf("invalid CAS helper output: %w\n%s", err, output)}
			return
		}
		results <- casProcessResult{Result: result}
	}()
	return results
}

func extractJSONLine(output []byte) []byte {
	for _, line := range bytes.Split(output, []byte{'\n'}) {
		trimmed := bytes.TrimSpace(line)
		if len(trimmed) > 0 && trimmed[0] == '{' {
			return trimmed
		}
	}
	return nil
}

func TestLocalCompareAndSwapSubprocessHelper(t *testing.T) {
	if os.Getenv("VAULTIC_LOCAL_CAS_HELPER") != "1" {
		t.Skip("helper subprocess")
	}
	repoPath := os.Getenv("VAULTIC_LOCAL_CAS_REPO")
	handleName := os.Getenv("VAULTIC_LOCAL_CAS_HANDLE")
	payload := os.Getenv("VAULTIC_LOCAL_CAS_PAYLOAD")
	if repoPath == "" || handleName == "" {
		t.Fatal("helper missing configuration")
	}

	be, err := local.Open(context.Background(), local.Config{Path: repoPath, Connections: 1}, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	defer be.Close()
	writer := backend.AsCapability[backend.ConditionalWriter](be)
	if writer == nil {
		t.Fatal("local backend does not expose conditional writer")
	}
	current, swapped, err := writer.CompareAndSwap(
		context.Background(),
		backend.Handle{Type: backend.StagingFile, Name: handleName},
		nil,
		[]byte(payload),
	)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = os.Stdout.Write(appendJSON(casResult{Current: string(current), Swapped: swapped}))
}

func appendJSON(result casResult) []byte {
	payload, _ := json.Marshal(result)
	return append(payload, '\n')
}
