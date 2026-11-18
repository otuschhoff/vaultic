package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

const publicationPerfMaxBytes = 256 << 20
const publicationPerfDrainTimeout = 60 * time.Second

type publicationPerfTrace struct {
	command *exec.Cmd
	cancel  context.CancelFunc
	control *os.File
	ack     *os.File
	reader  *bufio.Reader
	path    string
	pid     int
	started string
	ticks   string
	entries uint64
	exits   uint64
	context context.Context
}

func publicationPerfIdentity(pid int) (string, error) {
	if runtime.GOOS != "linux" || pid <= 1 {
		return "", errors.New("perf trace requires an owned Linux child")
	}
	stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return "", err
	}
	end := strings.LastIndexByte(string(stat), ')')
	if end < 0 {
		return "", errors.New("invalid child stat")
	}
	fields := strings.Fields(string(stat[end+1:]))
	if len(fields) <= 19 || fields[1] != strconv.Itoa(os.Getpid()) || strings.Contains("ZXx", fields[0]) {
		return "", errors.New("perf target is not a live direct child")
	}
	if ticks, err := strconv.ParseUint(fields[19], 10, 64); err != nil || ticks == 0 {
		return "", errors.New("perf target has invalid start ticks")
	}
	return fields[19], nil
}

func startPublicationPerfTrace(ctx context.Context, directory string, pid int, limit time.Duration) (*publicationPerfTrace, error) {
	if limit <= 0 || limit > 20*time.Minute {
		return nil, errors.New("perf trace requires a positive deadline of at most 20 minutes")
	}
	ticks, err := publicationPerfIdentity(pid)
	if err != nil {
		return nil, err
	}
	if err := os.Mkdir(directory, 0700); err != nil {
		return nil, err
	}
	if err := os.Chmod(directory, 0700); err != nil {
		return nil, err
	}
	logFile, err := os.OpenFile(filepath.Join(directory, "perf.log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	defer logFile.Close()
	if err := logFile.Chmod(0600); err != nil {
		return nil, err
	}
	controlRead, controlWrite, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	defer controlRead.Close()
	ackRead, ackWrite, err := os.Pipe()
	if err != nil {
		controlWrite.Close()
		return nil, err
	}
	defer ackWrite.Close()
	ctx, cancel := context.WithTimeout(ctx, limit)
	trace := &publicationPerfTrace{context: ctx, cancel: cancel, control: controlWrite, ack: ackRead,
		reader: bufio.NewReader(ackRead), path: filepath.Join(directory, "perf.data"), pid: pid, ticks: ticks}
	trace.command = exec.CommandContext(ctx, "/usr/bin/perf", "record", "--no-buildid", "-N", "--switch-events", "-D", "-1",
		"--control=fd:3,4", fmt.Sprintf("--max-size=%dM", publicationPerfMaxBytes>>20), "-o", trace.path,
		"-e", "syscalls:sys_enter_pread64,syscalls:sys_exit_pread64,nfs:nfs_file_read", "-p", strconv.Itoa(pid))
	trace.command.ExtraFiles = []*os.File{controlRead, ackWrite}
	trace.command.Stdout, trace.command.Stderr = logFile, logFile
	if err := trace.command.Start(); err != nil {
		trace.close()
		return nil, err
	}
	controlRead.Close()
	ackWrite.Close()
	if err := trace.setEnabled("enable"); err != nil {
		trace.close()
		return nil, err
	}
	trace.started = time.Now().UTC().Format(time.RFC3339Nano)
	return trace, nil
}

func (trace *publicationPerfTrace) setEnabled(action string) error {
	deadline := time.Now().Add(5 * time.Second)
	if err := trace.control.SetWriteDeadline(deadline); err != nil {
		return err
	}
	if err := trace.ack.SetReadDeadline(deadline); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(trace.control, action); err != nil {
		return fmt.Errorf("perf %s: %w", action, err)
	}
	ack, err := trace.reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("perf %s acknowledgment: %w", action, err)
	}
	if strings.Trim(ack, "\x00\r\n") != "ack" {
		return fmt.Errorf("unexpected perf %s acknowledgment %q", action, ack)
	}
	return nil
}

func (trace *publicationPerfTrace) close() {
	trace.cancel()
	trace.control.Close()
	trace.ack.Close()
	if trace.command.Process != nil && trace.command.ProcessState == nil {
		_ = trace.command.Wait()
	}
}

func auditPublicationPerfTrace(ctx context.Context, path string) (uint64, uint64, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, "/usr/bin/perf", "--no-pager", "script", "--show-lost-events", "-i", path)
	output, err := command.StdoutPipe()
	if err != nil {
		return 0, 0, err
	}
	command.Stderr = command.Stdout
	if err := command.Start(); err != nil {
		return 0, 0, err
	}
	var entries, exits uint64
	var lost bool
	scanner := bufio.NewScanner(output)
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case strings.Contains(line, "PERF_RECORD_LOST"):
			lost = true
		case strings.Contains(line, "syscalls:sys_enter_pread64:"):
			entries++
		case strings.Contains(line, "syscalls:sys_exit_pread64:"):
			exits++
		}
	}
	if err := scanner.Err(); err != nil {
		cancel()
		_ = command.Wait()
		return 0, 0, err
	}
	if err := command.Wait(); err != nil {
		return 0, 0, fmt.Errorf("decode perf trace: %w", err)
	}
	if lost || entries != exits {
		return 0, 0, fmt.Errorf("perf trace reported loss or unequal syscall counts: lost=%t, entries=%d, exits=%d", lost, entries, exits)
	}
	return entries, exits, nil
}

func (trace *publicationPerfTrace) finish(t *testing.T) error {
	defer trace.close()
	if err := trace.setEnabled("disable"); err != nil {
		return err
	}
	finished := time.Now().UTC().Format(time.RFC3339Nano)
	if err := trace.command.Process.Signal(os.Interrupt); err != nil {
		return err
	}
	timer := time.AfterFunc(publicationPerfDrainTimeout, trace.cancel)
	err := trace.command.Wait()
	if !timer.Stop() {
		return fmt.Errorf("perf recorder did not finalize within %s after disable", publicationPerfDrainTimeout)
	}
	if err != nil && trace.command.ProcessState.ExitCode() != 130 && trace.command.ProcessState.String() != "signal: interrupt" {
		return fmt.Errorf("perf recorder: %w", err)
	}
	ticks, err := publicationPerfIdentity(trace.pid)
	if err != nil || ticks != trace.ticks {
		return errors.New("perf target changed during capture")
	}
	info, err := os.Lstat(trace.path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0600 || info.Size() <= 0 || info.Size() >= publicationPerfMaxBytes {
		return fmt.Errorf("invalid perf artifact mode or size: %v, %d bytes", info.Mode(), info.Size())
	}
	trace.entries, trace.exits, err = auditPublicationPerfTrace(t.Context(), trace.path)
	if err != nil {
		return err
	}
	publicationRuntimeReceipt(t, filepath.Join(filepath.Dir(trace.path), "capture.json"), map[string]any{
		"pid": trace.pid, "start_ticks": trace.ticks, "enable_ack_utc": trace.started,
		"disable_ack_utc": finished, "perf_bytes": info.Size(), "loss_checked": true,
		"pread64_entries": trace.entries, "pread64_exits": trace.exits,
	})
	return nil
}

func (trace *publicationPerfTrace) preserveFailure(t *testing.T, directory string, failure error) {
	t.Helper()
	state := "unavailable"
	if trace.command.ProcessState != nil {
		state = trace.command.ProcessState.String()
	}
	sizes := make(map[string]int64)
	for _, artifact := range []struct {
		name  string
		limit int64
	}{{"perf.data", publicationPerfMaxBytes + (8 << 20)}, {"perf.log", 1 << 20}} {
		source := filepath.Join(filepath.Dir(trace.path), artifact.name)
		info, err := os.Lstat(source)
		if err != nil {
			continue
		}
		sizes[artifact.name] = info.Size()
		if info.Mode().IsRegular() && info.Mode().Perm() == 0600 && info.Size() <= artifact.limit {
			publicationQualificationCopy(t, filepath.Join(directory, "rejected-"+artifact.name), source)
		}
	}
	publicationRuntimeReceipt(t, filepath.Join(directory, "perf-failure.json"), map[string]any{
		"accepted": false, "error": failure.Error(), "process_state": state, "artifact_sizes": sizes,
		"deadline_exceeded": errors.Is(trace.context.Err(), context.DeadlineExceeded),
	})
}

func TestPublicationPerfTrace(t *testing.T) {
	if os.Getenv("VAULTIC_PUBLICATION_PERF_TEST_CHILD") == "1" {
		var trigger [1]byte
		if _, err := io.ReadFull(os.Stdin, trigger[:]); err != nil {
			t.Fatal(err)
		}
		file, err := os.Open("/etc/hosts")
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		var data [13]byte
		if _, err := file.ReadAt(data[:], 17); err != nil {
			t.Fatal(err)
		}
		fmt.Println("read finished")
		if _, err := io.ReadFull(os.Stdin, trigger[:]); err != nil {
			t.Fatal(err)
		}
		return
	}
	if os.Getenv("VAULTIC_TEST_PUBLICATION_PERF") != "1" || runtime.GOOS != "linux" {
		t.Skip("requires opt-in Linux perf tracepoint access")
	}
	if _, err := publicationPerfIdentity(os.Getpid()); err == nil {
		t.Fatal("accepted the qualifier itself as an owned child")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestPublicationPerfTrace$")
	child.Env = append(os.Environ(), "VAULTIC_PUBLICATION_PERF_TEST_CHILD=1")
	stdin, err := child.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := child.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	child.Stderr = os.Stderr
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if child.ProcessState == nil {
			_ = child.Process.Kill()
			_ = child.Wait()
		}
	}()
	directory := filepath.Join(t.TempDir(), "perf")
	trace, err := startPublicationPerfTrace(ctx, directory, child.Process.Pid, 12*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer trace.close()
	if _, err := stdin.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	if line, err := bufio.NewReader(stdout).ReadString('\n'); err != nil || line != "read finished\n" {
		t.Fatalf("fixture read did not finish: %q, %v", line, err)
	}
	if os.Getenv("VAULTIC_TEST_PUBLICATION_PERF_REJECT") == "1" {
		if err := trace.command.Process.Signal(os.Interrupt); err != nil {
			t.Fatal(err)
		}
		_ = trace.command.Wait()
		failure := trace.finish(t)
		if failure == nil {
			t.Fatal("accepted a recorder that exited before disable acknowledgment")
		}
		rejected := t.TempDir()
		trace.preserveFailure(t, rejected, failure)
		for _, name := range []string{"perf-failure.json", "rejected-perf.data", "rejected-perf.log"} {
			info, err := os.Stat(filepath.Join(rejected, name))
			if err != nil || info.Mode().Perm() != 0600 {
				t.Fatalf("missing private rejected artifact %s: %v", name, err)
			}
		}
		return
	}
	if err := trace.finish(t); err != nil {
		t.Fatal(err)
	}
	if trace.entries == 0 || trace.entries != trace.exits {
		t.Fatal("trace audit lost the positive smoke syscall")
	}
	if _, err := stdin.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	if err := child.Wait(); err != nil {
		t.Fatal(err)
	}
	output, err := exec.CommandContext(ctx, "/usr/bin/perf", "--no-pager", "script", "--show-lost-events", "-i", trace.path).CombinedOutput()
	text := string(output)
	if err != nil || strings.Contains(text, "PERF_RECORD_LOST") ||
		strings.Count(text, "count: 0x0000000d, pos: 0x00000011") != 1 ||
		strings.Count(text, "syscalls:sys_exit_pread64: 0xd\n") != 1 {
		t.Fatalf("unexpected smoke trace: %v\n%s", err, text)
	}
}
