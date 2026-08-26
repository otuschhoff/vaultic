package main

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/otuschhoff/vaultic/internal/global"
	rtest "github.com/otuschhoff/vaultic/internal/test"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
	"github.com/otuschhoff/vaultic/internal/vaultic"
	"github.com/spf13/pflag"
)

// testRunConfig runs the config command with the given --set-* arguments.
func testRunConfig(t testing.TB, gopts global.Options, args ...string) {
	t.Helper()
	var opts ConfigOptions
	fs := pflag.NewFlagSet("config", pflag.ContinueOnError)
	opts.AddFlags(fs)
	rtest.OK(t, fs.Parse(args))

	err := withTermStatus(t, gopts, func(ctx context.Context, gopts global.Options) error {
		return runConfig(ctx, fs, opts, gopts, nil, vaultic.NewNoopPrinter())
	})
	rtest.OK(t, err)
}

// testReadConfig returns the repository config as seen after opening the repo.
func testReadConfig(t testing.TB, gopts global.Options) (version, compression uint, dataPackSize uint64, extraVerify, appendOnly bool) {
	t.Helper()
	err := withTermStatus(t, gopts, func(ctx context.Context, gopts global.Options) error {
		ctx, repo, unlock, err := openWithReadLock(ctx, gopts, false, progress.NewTerminalPrinter(false, 0, gopts.Term))
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
	testRunInit(t, env.gopts)

	testRunConfig(t, env.gopts,
		"--set-compression", "10",
		"--set-extra-verify", "false",
		"--set-datapack-size", "33554432",
		"--set-min-packsize-tolerate-percent", "80")

	_, compression, dataPackSize, extraVerify, _ := testReadConfig(t, env.gopts)
	rtest.Equals(t, uint(10), compression)
	rtest.Equals(t, uint64(33554432), dataPackSize)
	rtest.Equals(t, false, extraVerify)

	// unset compression again
	testRunConfig(t, env.gopts, "--set-compression", "unset")
	_, compression, _, _, _ = testReadConfig(t, env.gopts)
	rtest.Equals(t, uint(0), compression)
}

func TestConfigInvalidValues(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	testRunInit(t, env.gopts)

	for _, args := range [][]string{
		{"--set-compression", "99"},
		{"--set-compression", "abc"},
		{"--set-append-only", "maybe"},
		{"--set-chunker", "bogus"},
		{"--set-min-packsize-tolerate-percent", "101"},
	} {
		var opts ConfigOptions
		fs := pflag.NewFlagSet("config", pflag.ContinueOnError)
		opts.AddFlags(fs)
		rtest.OK(t, fs.Parse(args))

		err := withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
			return runConfig(ctx, fs, opts, gopts, nil, vaultic.NewNoopPrinter())
		})
		rtest.Assert(t, err != nil, "expected error for %v", args)
	}
}

func TestInitSetConfig(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	err := withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
		return runInit(ctx, InitOptions{
			SetCompression:  "19",
			SetDataPackSize: "33554432",
			SetChunker:      "fixed_size",
			SetChunkSize:    "1048576",
		}, gopts, nil, gopts.Term)
	})
	rtest.OK(t, err)

	err = withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
		ctx, repo, unlock, err := openWithReadLock(ctx, gopts, false, progress.NewTerminalPrinter(false, 0, gopts.Term))
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
	testRunBackup(t, "", []string{target}, BackupOptions{}, env.gopts)

	// turn on append-only
	testRunConfig(t, env.gopts, "--set-append-only", "true")

	// a further backup (append) must still work
	testRunBackup(t, "", []string{target}, BackupOptions{}, env.gopts)

	// forget must fail to remove snapshots on an append-only repository
	var last ForgetPolicyCount
	rtest.OK(t, last.Set("1"))
	err := testRunForgetMayFail(t, env.gopts, ForgetOptions{Last: last})
	rtest.Assert(t, err != nil, "expected forget to fail on append-only repository")

	// config changes must be rejected too
	var opts ConfigOptions
	fs := pflag.NewFlagSet("config", pflag.ContinueOnError)
	opts.AddFlags(fs)
	rtest.OK(t, fs.Parse([]string{"--set-compression", "5"}))
	err = withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
		return runConfig(ctx, fs, opts, gopts, nil, vaultic.NewNoopPrinter())
	})
	rtest.Assert(t, err != nil && strings.Contains(err.Error(), "append-only"),
		"expected append-only error from config, got %v", err)
}
