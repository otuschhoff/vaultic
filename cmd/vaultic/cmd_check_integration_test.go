package main

import (
	"context"
	"strings"
	"testing"

	"github.com/otuschhoff/vaultic/internal/global"
	rtest "github.com/otuschhoff/vaultic/internal/test"
)

func testRunCheck(t testing.TB, globalOptions global.Options) {
	t.Helper()
	stdout, stderr, err := testRunCheckOutput(t, globalOptions, true)
	if err != nil {
		t.Error(stdout)
		t.Error(stderr)
		t.Fatalf("unexpected error: %+v", err)
	}
}

func testRunCheckMustFail(t testing.TB, globalOptions global.Options) {
	t.Helper()
	_, _, err := testRunCheckOutput(t, globalOptions, false)
	rtest.Assert(t, err != nil, "expected non nil error after check of damaged repository")
}

func testRunCheckOutput(t testing.TB, globalOptions global.Options, checkUnused bool) (string, string, error) {
	stdout, stderr, err := withCaptureStdoutStderr(t, globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		options := checkOptions{
			ReadData:    true,
			CheckUnused: checkUnused,
		}
		_, err := runCheck(ctx, options, globalOptions, nil, globalOptions.Term)
		return err
	})
	return stdout.String(), stderr.String(), err
}

func testRunCheckOutputWithOpts(t testing.TB, globalOptions global.Options, options checkOptions, args []string) (string, error) {
	buf, err := withCaptureStdout(t, globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		globalOptions.Verbosity = 2
		_, err := runCheck(ctx, options, globalOptions, args, globalOptions.Term)
		return err
	})
	return buf.String(), err
}

func TestCheckWithSnaphotFilter(t *testing.T) {
	testCases := []struct {
		options        checkOptions
		args           []string
		expectedOutput string
	}{
		{ // full --read-data, all snapshots
			checkOptions{ReadData: true},
			nil,
			"4 / 4 packs",
		},
		{ // full --read-data, all snapshots
			checkOptions{ReadData: true},
			nil,
			"2 / 2 snapshots",
		},
		{ // full --read-data, latest snapshot
			checkOptions{ReadData: true},
			[]string{"latest"},
			"2 / 2 packs",
		},
		{ // full --read-data, latest snapshot
			checkOptions{ReadData: true},
			[]string{"latest"},
			"1 / 1 snapshots",
		},
		{ // --read-data-subset, latest snapshot
			checkOptions{ReadDataSubset: "1%"},
			[]string{"latest"},
			"1 / 1 packs",
		},
		{ // --read-data-subset, latest snapshot
			checkOptions{ReadDataSubset: "1%"},
			[]string{"latest"},
			"filtered",
		},
	}

	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	testSetupBackupData(t, env)
	options := backupOptions{}
	testRunBackup(t, env.testdata+"/0", []string{"for_cmd_ls"}, options, env.globalOptions)
	testRunBackup(t, env.testdata+"/0", []string{"0/9"}, options, env.globalOptions)

	for _, testCase := range testCases {
		output, err := testRunCheckOutputWithOpts(t, env.globalOptions, testCase.options, testCase.args)
		rtest.OK(t, err)

		hasOutput := strings.Contains(output, testCase.expectedOutput)
		rtest.Assert(t, hasOutput, `expected to find substring %q, but did not find it`, testCase.expectedOutput)
	}
}
