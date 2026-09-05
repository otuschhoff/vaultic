package main

import (
	"context"
	"strings"
	"testing"

	"github.com/otuschhoff/vaultic/internal/global"
	rtest "github.com/otuschhoff/vaultic/internal/test"
)

func testRunGenerate(t testing.TB, globalOptions global.Options, options generateOptions) ([]byte, error) {
	buf, err := withCaptureStdout(t, globalOptions, func(ctx context.Context, capturedOptions global.Options) error {
		return runGenerate(options, capturedOptions, []string{}, capturedOptions.Term)
	})
	return buf.Bytes(), err
}

func TestGenerateStdout(t *testing.T) {
	testCases := []struct {
		name    string
		options generateOptions
	}{
		{"bash", generateOptions{BashCompletionFile: "-"}},
		{"fish", generateOptions{FishCompletionFile: "-"}},
		{"zsh", generateOptions{ZSHCompletionFile: "-"}},
		{"powershell", generateOptions{PowerShellCompletionFile: "-"}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			output, err := testRunGenerate(t, global.Options{}, tc.options)
			rtest.OK(t, err)
			rtest.Assert(t, strings.Contains(string(output), "# "+tc.name+" completion for vaultic"), "has no expected completion header")
		})
	}

	t.Run("Generate shell completions to stdout for two shells", func(t *testing.T) {
		_, err := testRunGenerate(t, global.Options{}, generateOptions{BashCompletionFile: "-", FishCompletionFile: "-"})
		rtest.Assert(t, err != nil, "generate shell completions to stdout for two shells fails")
	})
}
