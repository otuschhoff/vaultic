package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/otuschhoff/vaultic/internal/global"
	rtest "github.com/otuschhoff/vaultic/internal/test"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
	"github.com/otuschhoff/vaultic/internal/vaultic"
	"github.com/otuschhoff/vaultic/internal/workingkv"
	"github.com/spf13/pflag"
)

func TestM4WorkingMemoryCommandRoundTrip(t *testing.T) {
	t.Setenv("VAULTIC_PASSWORD", rtest.TestPassword)
	for _, mode := range []string{"ram", "kv", "auto"} {
		t.Run(mode, func(t *testing.T) {
			environment, cleanup := withTestEnvironment(t)
			defer cleanup()
			testRunInit(t, environment.globalOptions)
			input := filepath.Join(environment.testdata, "m4-bytes")
			expected := []byte("text\x00binary\xff\n")
			if err := os.WriteFile(input, expected, 0600); err != nil {
				t.Fatal(err)
			}
			scratch := filepath.Join(environment.base, "working")
			if err := os.Mkdir(scratch, 0700); err != nil {
				t.Fatal(err)
			}
			err := withTermStatus(t, environment.globalOptions, func(ctx context.Context, options global.Options) error {
				saved := options
				root := newRootCommand(&options)
				options = saved
				root.SetArgs([]string{"backup", "--working-memory-mode=" + mode, "--working-memory-ram-limit=32MiB", "--working-memory-scratch=" + scratch, input})
				return root.ExecuteContext(ctx)
			})
			if err != nil {
				t.Fatal(err)
			}
			output := filepath.Join(environment.base, "restored")
			testRunRestore(t, environment.globalOptions, output, "latest")
			restored, err := os.ReadFile(filepath.Join(output, strings.TrimPrefix(input, "/")))
			if err != nil || string(restored) != string(expected) {
				t.Fatal("command restore bytes differ", err, restored)
			}
			files, err := os.ReadDir(scratch)
			if err != nil || len(files) != 0 {
				t.Fatal("command scratch leaked", err)
			}
			err = withTermStatus(t, environment.globalOptions, func(ctx context.Context, options global.Options) error {
				saved := options
				root := newRootCommand(&options)
				options = saved
				root.SetArgs([]string{"backup", "--working-memory-mode=" + mode, "--working-memory-ram-limit=1KiB", "--working-memory-scratch=" + scratch, input})
				return root.ExecuteContext(ctx)
			})
			if !errors.Is(err, workingkv.ErrWorkingMemoryLimitExceeded) {
				t.Fatal("unsafe startup succeeded", err)
			}
		})
	}
}

func TestM4WorkingMemoryEnvironmentPrecedence(t *testing.T) {
	t.Setenv("VAULTIC_WORKING_MEMORY_MODE", "ram")
	t.Setenv("RESTIC_WORKING_MEMORY_MODE", "kv")
	t.Setenv("VAULTIC_WORKING_MEMORY_RAM_LIMIT", "32MiB")
	options := global.Options{}
	root := newRootCommand(&options)
	command, _, err := root.Find([]string{"backup"})
	if err != nil {
		t.Fatal(err)
	}
	if options.WorkingMemory.Mode != "ram" || options.WorkingMemory.RAMLimit != "32MiB" {
		t.Fatal(options.WorkingMemory)
	}
	if err := command.ParseFlags([]string{"--working-memory-mode=auto"}); err != nil {
		t.Fatal(err)
	}
	if options.WorkingMemory.Mode != "auto" {
		t.Fatal("CLI lost to environment")
	}
}

func TestM4WorkingMemoryProfileConflictAndStop(t *testing.T) {
	path := filepath.Join(t.TempDir(), "conflict.toml")
	if err := os.WriteFile(path, []byte("[working_memory]\nmode=\"ram\"\nram_limit=\"32MiB\"\n[backup]\nmetadata-scratch=\"/unused/conflict\"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	options := global.Options{}
	root := newRootCommand(&options)
	options.UseProfiles = []string{path}
	command, _, err := root.Find([]string{"backup"})
	if err != nil {
		t.Fatal(err)
	}
	if err := applyProfile(command, &options); err != nil {
		t.Fatal(err)
	}
	policy, _ := workingkv.NewPolicy(workingkv.ModeRAM, 32<<20, "")
	if err := global.ValidateWorkingMemoryFlags(command.Flags(), policy); !errors.Is(err, workingkv.ErrWorkingPolicyConflict) {
		t.Fatal("profile conflict bypassed shared policy", err)
	}
	ctx, cancel := context.WithCancelCause(workingkv.WithPolicy(t.Context(), policy))
	cancel(workingkv.ErrWorkingMemoryLimitExceeded)
	err = global.WorkingMemoryError(nil, ctx)
	if !errors.Is(err, workingkv.ErrWorkingMemoryLimitExceeded) || !strings.Contains(err.Error(), "configured_limit=33554432") {
		t.Fatal("resource stop falsely succeeded or hid limit", err)
	}
}

func TestM4WorkingMemoryProfilePrecedence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "working.toml")
	if err := os.WriteFile(path, []byte("[working_memory]\nmode = \"auto\"\nram_limit = \"16MiB\"\nscratch = \"/unused/profile/scratch\"\ndisk_budget = \"64MiB\"\nreserve = \"128MiB\"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"backup", "index import", "index check", "check"} {
		t.Run(name, func(t *testing.T) {
			options := global.Options{UseProfiles: []string{path}}
			root := newRootCommand(&options)
			options.UseProfiles = []string{path}
			command, _, err := root.Find(strings.Fields(name))
			if err != nil {
				t.Fatal(err)
			}
			if err := command.ParseFlags([]string{"--working-memory-mode=ram", "--working-memory-ram-limit=32MiB"}); err != nil {
				t.Fatal(err)
			}
			if err := applyProfile(command, &options); err != nil {
				t.Fatal(err)
			}
			if options.WorkingMemory.Mode != "ram" || options.WorkingMemory.RAMLimit != "32MiB" || options.WorkingMemory.DiskBudget != "64MiB" {
				t.Fatal("profile overrode CLI", options.WorkingMemory)
			}
			policy, err := workingkv.Resolve(options.WorkingMemory, workingkv.Resources{HostLimit: 2 << 30, HostAvailable: 1 << 30, RSSKnown: true})
			if err != nil {
				t.Fatal(err)
			}
			if err := global.ValidateWorkingMemoryFlags(command.Flags(), policy); err != nil {
				t.Fatal(err)
			}
			if command.Flags().Lookup("metadata-scratch") != nil {
				if err := command.Flags().Set("metadata-scratch", "conflicting"); err != nil {
					t.Fatal(err)
				}
				if err := global.ValidateWorkingMemoryFlags(command.Flags(), policy); !errors.Is(err, workingkv.ErrWorkingPolicyConflict) {
					t.Fatal("legacy conflict accepted", err)
				}
			}
		})
	}
}

// testRunConfig runs the config command with the given --set-* arguments.
func testRunConfig(t testing.TB, globalOptions global.Options, args ...string) {
	t.Helper()
	var options configOptions
	fs := pflag.NewFlagSet("config", pflag.ContinueOnError)
	options.AddFlags(fs)
	rtest.OK(t, fs.Parse(args))

	err := withTermStatus(t, globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		return runConfig(ctx, fs, options, globalOptions, nil, vaultic.NewNoopPrinter())
	})
	rtest.OK(t, err)
}

// testReadConfig returns the repository config as seen after opening the repo.
func testReadConfig(t testing.TB, globalOptions global.Options) (version, compression uint, dataPackSize uint64, extraVerify, appendOnly bool) {
	t.Helper()
	err := withTermStatus(t, globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		ctx, repo, unlock, err := openWithReadLock(ctx, globalOptions, false, progress.NewTerminalPrinter(false, 0, globalOptions.Term))
		rtest.OK(t, err)
		defer unlock()
		cfg := repo.Config()
		_ = ctx
		if cfg.Compression != nil {
			compression = uint(*cfg.Compression)
		}
		dataPackSize = cfg.DataPackSizeBytes
		extraVerify = cfg.ExtraVerifyEnabled()
		appendOnly = cfg.AppendOnly()
		return nil
	})
	rtest.OK(t, err)
	return version, compression, dataPackSize, extraVerify, appendOnly
}

func TestConfigCommand(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	testRunInit(t, env.globalOptions)

	testRunConfig(t, env.globalOptions,
		"--set-compression", "10",
		"--set-extra-verify", "false",
		"--set-datapack-size", "33554432",
		"--set-min-packsize-tolerate-percent", "80")

	_, compression, dataPackSize, extraVerify, _ := testReadConfig(t, env.globalOptions)
	rtest.Equals(t, uint(10), compression)
	rtest.Equals(t, uint64(33554432), dataPackSize)
	rtest.Equals(t, false, extraVerify)

	// unset compression again
	testRunConfig(t, env.globalOptions, "--set-compression", "unset")
	_, compression, _, _, _ = testReadConfig(t, env.globalOptions)
	rtest.Equals(t, uint(0), compression)
}

func TestConfigInvalidValues(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	testRunInit(t, env.globalOptions)

	for _, args := range [][]string{
		{"--set-compression", "99"},
		{"--set-compression", "abc"},
		{"--set-append-only", "maybe"},
		{"--set-chunker", "bogus"},
		{"--set-min-packsize-tolerate-percent", "101"},
	} {
		var options configOptions
		fs := pflag.NewFlagSet("config", pflag.ContinueOnError)
		options.AddFlags(fs)
		rtest.OK(t, fs.Parse(args))

		err := withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
			return runConfig(ctx, fs, options, globalOptions, nil, vaultic.NewNoopPrinter())
		})
		rtest.Assert(t, err != nil, "expected error for %v", args)
	}
}

func TestInitSetConfig(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	err := withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		return runInit(ctx, initOptions{
			SetCompression:  "19",
			SetDataPackSize: "33554432",
			SetChunker:      "fixed_size",
			SetChunkSize:    "1048576",
		}, globalOptions, nil, globalOptions.Term)
	})
	rtest.OK(t, err)

	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		ctx, repo, unlock, err := openWithReadLock(ctx, globalOptions, false, progress.NewTerminalPrinter(false, 0, globalOptions.Term))
		rtest.OK(t, err)
		defer unlock()
		cfg := repo.Config()
		rtest.Assert(t, cfg.Compression != nil && *cfg.Compression == 19, "compression from init not persisted")
		rtest.Equals(t, uint64(33554432), cfg.DataPackSizeBytes)
		rtest.Equals(t, "FixedSize", string(cfg.Chunker()))
		rtest.Equals(t, uint64(1048576), cfg.ChunkSize())
		_ = ctx
		return nil
	})
	rtest.OK(t, err)
}

func TestAppendOnlyBlocksForgetAndConfig(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	testSetupBackupData(t, env)
	target := filepath.Join(env.testdata, "0", "0", "9")
	testRunBackup(t, "", []string{target}, backupOptions{}, env.globalOptions)

	// turn on append-only
	testRunConfig(t, env.globalOptions, "--set-append-only", "true")

	// a further backup (append) must still work
	testRunBackup(t, "", []string{target}, backupOptions{}, env.globalOptions)

	// forget must fail to remove snapshots on an append-only repository
	var last ForgetPolicyCount
	rtest.OK(t, last.Set("1"))
	err := testRunForgetMayFail(t, env.globalOptions, forgetOptions{Last: last})
	rtest.Assert(t, err != nil, "expected forget to fail on append-only repository")

	// config changes must be rejected too
	var options configOptions
	fs := pflag.NewFlagSet("config", pflag.ContinueOnError)
	options.AddFlags(fs)
	rtest.OK(t, fs.Parse([]string{"--set-compression", "5"}))
	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		return runConfig(ctx, fs, options, globalOptions, nil, vaultic.NewNoopPrinter())
	})
	rtest.Assert(t, err != nil && strings.Contains(err.Error(), "append-only"),
		"expected append-only error from config, got %v", err)
}
