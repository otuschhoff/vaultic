//go:build debug || profile

package global

import (
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"runtime/trace"

	"github.com/otuschhoff/vaultic/internal/errors"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/pkg/profile"
)

func RegisterProfiling(cmd *cobra.Command, stderr io.Writer) {
	var profiler Profiler

	origPreRun := cmd.PersistentPreRunE
	cmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		if origPreRun != nil {
			if err := origPreRun(cmd, args); err != nil {
				return err
			}
		}
		return profiler.Start(profiler.opts, stderr)
	}

	// Once https://github.com/spf13/cobra/issues/1893 is fixed,
	// this could use PersistentPostRunE instead of OnFinalize,
	// reverting https://github.com/otuschhoff/vaultic/pull/5373.
	cobra.OnFinalize(func() {
		profiler.Stop()
	})

	profiler.opts.AddFlags(cmd.PersistentFlags())
}

type Profiler struct {
	opts ProfileOptions
	stop interface {
		Stop()
	}
}

type ProfileOptions struct {
	listen        string
	memPath       string
	cpuPath       string
	tracePath     string
	traceMaxBytes uint64
	blockPath     string
	insecure      bool
}

func (opts *ProfileOptions) AddFlags(f *pflag.FlagSet) {
	f.StringVar(&opts.listen, "listen-profile", "", "listen on this `address:port` for memory profiling")
	f.StringVar(&opts.memPath, "mem-profile", "", "write memory profile to `dir`")
	f.StringVar(&opts.cpuPath, "cpu-profile", "", "write cpu profile to `dir`")
	f.StringVar(&opts.tracePath, "trace-profile", "", "write trace to `dir`")
	f.Uint64Var(&opts.traceMaxBytes, "trace-profile-max-bytes", 0, "cap an opt-in runtime trace and require a completion receipt")
	f.StringVar(&opts.blockPath, "block-profile", "", "write block profile to `dir`")
	f.BoolVar(&opts.insecure, "insecure-kdf", false, "use insecure KDF settings")
}

type fakeTestingTB struct {
	stderr io.Writer
}

func (t fakeTestingTB) Logf(msg string, args ...interface{}) {
	fmt.Fprintf(t.stderr, msg, args...)
}

func (p *Profiler) Start(profileOpts ProfileOptions, stderr io.Writer) error {
	if profileOpts.listen != "" {
		fmt.Fprintf(stderr, "running profile HTTP server on %v\n", profileOpts.listen)
		go func() {
			err := http.ListenAndServe(profileOpts.listen, nil)
			if err != nil {
				fmt.Fprintf(stderr, "profile HTTP server listen failed: %v\n", err)
			}
		}()
	}

	profilesEnabled := 0
	if profileOpts.memPath != "" {
		profilesEnabled++
	}
	if profileOpts.cpuPath != "" {
		profilesEnabled++
	}
	if profileOpts.tracePath != "" {
		profilesEnabled++
	}
	if profileOpts.blockPath != "" {
		profilesEnabled++
	}

	if profilesEnabled > 1 {
		return errors.Fatal("only one profile (memory, CPU, trace, or block) may be activated at the same time")
	}
	if profileOpts.traceMaxBytes != 0 && profileOpts.tracePath == "" {
		return errors.Fatal("trace-profile-max-bytes requires trace-profile")
	}

	if profileOpts.memPath != "" {
		p.stop = profile.Start(profile.Quiet, profile.NoShutdownHook, profile.MemProfile, profile.ProfilePath(profileOpts.memPath))
	} else if profileOpts.cpuPath != "" {
		p.stop = profile.Start(profile.Quiet, profile.NoShutdownHook, profile.CPUProfile, profile.ProfilePath(profileOpts.cpuPath))
	} else if profileOpts.tracePath != "" {
		if profileOpts.traceMaxBytes == 0 {
			p.stop = profile.Start(profile.Quiet, profile.NoShutdownHook, profile.TraceProfile, profile.ProfilePath(profileOpts.tracePath))
		} else {
			bounded, err := startBoundedRuntimeTrace(profileOpts.tracePath, profileOpts.traceMaxBytes, stderr)
			if err != nil {
				return err
			}
			p.stop = bounded
		}
	} else if profileOpts.blockPath != "" {
		p.stop = profile.Start(profile.Quiet, profile.NoShutdownHook, profile.BlockProfile, profile.ProfilePath(profileOpts.blockPath))
	}

	if profileOpts.insecure {
		repository.TestUseLowSecurityKDFParameters(fakeTestingTB{stderr})
	}

	return nil
}

func (p *Profiler) Stop() {
	if p.stop != nil {
		p.stop.Stop()
	}
}

type boundedRuntimeTrace struct {
	file      *os.File
	remaining uint64
	written   uint64
	failure   error
	stderr    io.Writer
}

func startBoundedRuntimeTrace(directory string, limit uint64, stderr io.Writer) (*boundedRuntimeTrace, error) {
	if _, err := os.Lstat(filepath.Join(directory, "trace.complete")); !os.IsNotExist(err) {
		return nil, fmt.Errorf("bounded runtime trace requires a fresh completion path: %v", err)
	}
	file, err := os.OpenFile(filepath.Join(directory, "trace.out"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	if err := file.Chmod(0600); err != nil {
		file.Close()
		return nil, err
	}
	bounded := &boundedRuntimeTrace{file: file, remaining: limit, stderr: stderr}
	if err := trace.Start(bounded); err != nil {
		file.Close()
		return nil, err
	}
	return bounded, nil
}

func (bounded *boundedRuntimeTrace) Write(data []byte) (int, error) {
	if bounded.failure != nil {
		return 0, bounded.failure
	}
	if uint64(len(data)) > bounded.remaining {
		bounded.failure = fmt.Errorf("runtime trace exceeded its byte limit")
		return 0, bounded.failure
	}
	count, err := bounded.file.Write(data)
	bounded.failure = err
	bounded.written += uint64(count)
	bounded.remaining -= uint64(count)
	return count, err
}

func (bounded *boundedRuntimeTrace) Stop() {
	trace.Stop()
	closeErr := bounded.file.Close()
	if bounded.failure != nil || closeErr != nil || bounded.written == 0 {
		fmt.Fprintf(bounded.stderr, "bounded runtime trace rejected: write=%v close=%v bytes=%d\n", bounded.failure, closeErr, bounded.written)
		return
	}
	path := filepath.Join(filepath.Dir(bounded.file.Name()), "trace.complete")
	receipt, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err == nil {
		err = receipt.Chmod(0600)
		if err == nil {
			_, err = fmt.Fprintln(receipt, bounded.written)
		}
		closeErr := receipt.Close()
		if err == nil {
			err = closeErr
		}
		if err != nil {
			_ = os.Remove(path)
		}
	}
	if err != nil {
		fmt.Fprintf(bounded.stderr, "bounded runtime trace receipt rejected: %v\n", err)
	}
}
