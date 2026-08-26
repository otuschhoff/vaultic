// Package warmup implements vaultic's cold-storage warm-up command support.
//
// Cold-storage backends (e.g. S3 Glacier, OVH Cold Archive) need objects to be
// "warmed up" (restored) before they can be read. There is no vendor-neutral
// protocol for that, so vaultic invokes a user-supplied warm-up program for the
// pack files it is about to read. This mirrors rustic's --warm-up-* options
// (see doc/rustic-parity-roadmap.md, workstream WS-D / Appendix A).
package warmup

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/errors"
)

// Options configures the warm-up command.
type Options struct {
	// Command is the warm-up program to invoke. The variables %id, %path,
	// %ids and %paths are substituted (see below). Empty disables warm-up.
	Command string
	// Batch is the warm-up batch size: how many packs are passed to one
	// invocation of the command (%ids/%paths) or how many parallel invocations
	// are run (%id/%path). 0 means 1.
	Batch int
	// Wait is the maximum time to wait for the packs to become available after
	// the command returned (used when the command returns before the data is
	// actually readable). 0 disables waiting.
	Wait time.Duration
	// WaitCommand is an optional program that is run to wait until the
	// requested packs are available (alternative to Wait).
	WaitCommand string
}

// progressMessage is the JSON-lines message emitted by the warm-up program.
// Only "pack-progress" is currently defined.
type progressMessage struct {
	Type string `json:"type"`
	Warm int    `json:"warm"`
}

// Runner runs a warm-up command for batches of pack handles.
type Runner struct {
	opts Options
	// progressFn is called with the cumulative number of warm packs per batch.
	progressFn func(warm, total int)
	// logFn receives non-JSON output lines of the warm-up program.
	logFn func(msg string)
}

// New returns a Runner. progressFn may be nil; logFn defaults to a no-op.
func New(opts Options, progressFn func(warm, total int), logFn func(msg string)) *Runner {
	if opts.Batch <= 0 {
		opts.Batch = 1
	}
	if logFn == nil {
		logFn = func(string) {}
	}
	return &Runner{opts: opts, progressFn: progressFn, logFn: logFn}
}

// Enabled reports whether a warm-up command is configured.
func (r *Runner) Enabled() bool {
	return r != nil && r.opts.Command != ""
}

// Warmup warms up the given handles by invoking the configured command in
// batches. It blocks until all invocations completed (and, when --warm-up-wait
// or --warm-up-wait-command are set, until the wait condition is satisfied).
//
// Variables substituted into the command:
//
//	%id     - one pack ID per invocation (%ids: several)
//	%path   - one backend path per invocation (%paths: several)
//	%ids    - a batch of pack IDs (space-separated) per invocation
//	%paths  - a batch of backend paths (space-separated) per invocation
func (r *Runner) Warmup(ctx context.Context, handles []backend.Handle, pathFor func(backend.Handle) string) error {
	if !r.Enabled() || len(handles) == 0 {
		return nil
	}

	batch := r.opts.Batch
	useBatch := strings.Contains(r.opts.Command, "%ids") || strings.Contains(r.opts.Command, "%paths")

	if useBatch {
		// invoke once per batch of N handles
		for start := 0; start < len(handles); start += batch {
			end := min(start+batch, len(handles))
			if err := r.runBatch(ctx, handles[start:end], pathFor); err != nil {
				return err
			}
		}
	} else {
		// %id / %path: run up to `batch` parallel invocations, one handle each
		if err := r.runParallel(ctx, handles, pathFor); err != nil {
			return err
		}
	}

	return r.wait(ctx, handles, pathFor)
}

// runBatch invokes the command once for a batch of handles (%ids/%paths).
func (r *Runner) runBatch(ctx context.Context, handles []backend.Handle, pathFor func(backend.Handle) string) error {
	ids := make([]string, 0, len(handles))
	paths := make([]string, 0, len(handles))
	for _, h := range handles {
		ids = append(ids, h.Name)
		paths = append(paths, pathFor(h))
	}
	return r.invoke(ctx, ids, paths)
}

// runParallel runs one invocation per handle, up to opts.Batch in parallel.
func (r *Runner) runParallel(ctx context.Context, handles []backend.Handle, pathFor func(backend.Handle) string) error {
	var wg sync.WaitGroup
	sem := make(chan struct{}, r.opts.Batch)
	errCh := make(chan error, len(handles))

	for _, h := range handles {
		h := h
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
				if err := r.invoke(ctx, []string{h.Name}, []string{pathFor(h)}); err != nil {
					select {
					case errCh <- err:
					case <-ctx.Done():
					}
				}
			case <-ctx.Done():
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return ctx.Err()
}

// invoke runs the warm-up command once with the given ids/paths substituted.
func (r *Runner) invoke(ctx context.Context, ids, paths []string) error {
	cmdline := r.opts.Command
	repl := map[string]string{
		"%ids":   strings.Join(ids, " "),
		"%paths": strings.Join(paths, " "),
		"%id":    first(ids),
		"%path":  first(paths),
	}
	// substitute longer placeholders first so %ids is not partially replaced by %id
	for _, k := range []string{"%ids", "%paths", "%id", "%path"} {
		cmdline = strings.ReplaceAll(cmdline, k, repl[k])
	}

	cmd := exec.CommandContext(ctx, "sh", "-c", cmdline)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	cmd.Stderr = nil // let stderr go to the parent's stderr

	if err := cmd.Start(); err != nil {
		return errors.Wrap(err, "warm-up command failed to start")
	}

	// parse JSON-lines progress from stdout
	warm := 0
	sc := bufio.NewScanner(stdout)
	for sc.Scan() {
		line := sc.Text()
		var msg progressMessage
		if err := json.Unmarshal([]byte(line), &msg); err == nil && msg.Type == "pack-progress" {
			if msg.Warm > warm {
				warm = msg.Warm
				if r.progressFn != nil {
					r.progressFn(warm, len(ids))
				}
			}
		} else if strings.TrimSpace(line) != "" {
			r.logFn(fmt.Sprintf("[warmup] %s", line))
		}
	}

	err = cmd.Wait()
	// a program that never reported progress counts the whole batch as done on success
	if err == nil && r.progressFn != nil && warm < len(ids) {
		r.progressFn(len(ids), len(ids))
	}
	if err != nil {
		return errors.Wrap(err, "warm-up command failed")
	}
	return nil
}

// wait implements --warm-up-wait and --warm-up-wait-command.
func (r *Runner) wait(ctx context.Context, handles []backend.Handle, pathFor func(backend.Handle) string) error {
	if r.opts.WaitCommand != "" {
		ids := make([]string, 0, len(handles))
		paths := make([]string, 0, len(handles))
		for _, h := range handles {
			ids = append(ids, h.Name)
			paths = append(paths, pathFor(h))
		}
		cmdline := substitute(r.opts.WaitCommand, ids, paths)
		cmd := exec.CommandContext(ctx, "sh", "-c", cmdline)
		if out, err := cmd.CombinedOutput(); err != nil {
			return errors.Wrapf(err, "warm-up wait command failed: %s", strings.TrimSpace(string(out)))
		}
		return nil
	}

	if r.opts.Wait > 0 {
		timer := time.NewTimer(r.opts.Wait)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return nil
		}
	}
	return nil
}

func first(s []string) string {
	if len(s) == 0 {
		return ""
	}
	return s[0]
}

func substitute(cmdline string, ids, paths []string) string {
	repl := map[string]string{
		"%ids":   strings.Join(ids, " "),
		"%paths": strings.Join(paths, " "),
		"%id":    first(ids),
		"%path":  first(paths),
	}
	for _, k := range []string{"%ids", "%paths", "%id", "%path"} {
		cmdline = strings.ReplaceAll(cmdline, k, repl[k])
	}
	return cmdline
}
