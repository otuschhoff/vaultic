package main

import (
	"testing"

	"github.com/otuschhoff/vaultic/internal/data"
	rtest "github.com/otuschhoff/vaultic/internal/test"
	"github.com/spf13/pflag"
)

func TestForgetPolicyValues(t *testing.T) {
	testCases := []struct {
		input string
		value ForgetPolicyCount
		err   string
	}{
		{"0", ForgetPolicyCount(0), ""},
		{"1", ForgetPolicyCount(1), ""},
		{"unlimited", ForgetPolicyCount(-1), ""},
		{"", ForgetPolicyCount(0), "strconv.ParseInt: parsing \"\": invalid syntax"},
		{"-1", ForgetPolicyCount(0), ErrNegativePolicyCount.Error()},
		{"abc", ForgetPolicyCount(0), "strconv.ParseInt: parsing \"abc\": invalid syntax"},
	}
	for _, testCase := range testCases {
		t.Run("", func(t *testing.T) {
			var count ForgetPolicyCount
			err := count.Set(testCase.input)

			if testCase.err != "" {
				rtest.Assert(t, err != nil, "should have returned error for input %+v", testCase.input)
				rtest.Equals(t, testCase.err, err.Error())
			} else {
				rtest.Assert(t, err == nil, "expected no error for input %+v, got %v", testCase.input, err)
				rtest.Equals(t, testCase.value, count)
				rtest.Equals(t, testCase.input, count.String())
			}
		})
	}
}

func TestForgetOptionValues(t *testing.T) {
	const negValErrorMsg = "Fatal: negative values other than -1 are not allowed for --keep-*"
	const negDurationValErrorMsg = "Fatal: durations containing negative values are not allowed for --keep-within*"
	testCases := []struct {
		input    forgetOptions
		errorMsg string
	}{
		{forgetOptions{Last: 1}, ""},
		{forgetOptions{Hourly: 1}, ""},
		{forgetOptions{Daily: 1}, ""},
		{forgetOptions{Weekly: 1}, ""},
		{forgetOptions{Monthly: 1}, ""},
		{forgetOptions{Yearly: 1}, ""},
		{forgetOptions{Last: 0}, ""},
		{forgetOptions{Hourly: 0}, ""},
		{forgetOptions{Daily: 0}, ""},
		{forgetOptions{Weekly: 0}, ""},
		{forgetOptions{Monthly: 0}, ""},
		{forgetOptions{Yearly: 0}, ""},
		{forgetOptions{Last: -1}, ""},
		{forgetOptions{Hourly: -1}, ""},
		{forgetOptions{Daily: -1}, ""},
		{forgetOptions{Weekly: -1}, ""},
		{forgetOptions{Monthly: -1}, ""},
		{forgetOptions{Yearly: -1}, ""},
		{forgetOptions{Last: -2}, negValErrorMsg},
		{forgetOptions{Hourly: -2}, negValErrorMsg},
		{forgetOptions{Daily: -2}, negValErrorMsg},
		{forgetOptions{Weekly: -2}, negValErrorMsg},
		{forgetOptions{Monthly: -2}, negValErrorMsg},
		{forgetOptions{Yearly: -2}, negValErrorMsg},
		{forgetOptions{Within: data.ParseDurationOrPanic("1y2m3d3h")}, ""},
		{forgetOptions{WithinHourly: data.ParseDurationOrPanic("1y2m3d3h")}, ""},
		{forgetOptions{WithinDaily: data.ParseDurationOrPanic("1y2m3d3h")}, ""},
		{forgetOptions{WithinWeekly: data.ParseDurationOrPanic("1y2m3d3h")}, ""},
		{forgetOptions{WithinMonthly: data.ParseDurationOrPanic("2y4m6d8h")}, ""},
		{forgetOptions{WithinYearly: data.ParseDurationOrPanic("2y4m6d8h")}, ""},
		{forgetOptions{Within: data.ParseDurationOrPanic("-1y2m3d3h")}, negDurationValErrorMsg},
		{forgetOptions{WithinHourly: data.ParseDurationOrPanic("1y-2m3d3h")}, negDurationValErrorMsg},
		{forgetOptions{WithinDaily: data.ParseDurationOrPanic("1y2m-3d3h")}, negDurationValErrorMsg},
		{forgetOptions{WithinWeekly: data.ParseDurationOrPanic("1y2m3d-3h")}, negDurationValErrorMsg},
		{forgetOptions{WithinMonthly: data.ParseDurationOrPanic("-2y4m6d8h")}, negDurationValErrorMsg},
		{forgetOptions{WithinYearly: data.ParseDurationOrPanic("2y-4m6d8h")}, negDurationValErrorMsg},
	}

	for _, testCase := range testCases {
		err := verifyForgetOptions(&testCase.input)
		if testCase.errorMsg != "" {
			rtest.Assert(t, err != nil, "should have returned error for input %+v", testCase.input)
			rtest.Equals(t, testCase.errorMsg, err.Error())
		} else {
			rtest.Assert(t, err == nil, "expected no error for input %+v", testCase.input)
		}
	}
}

func TestForgetHostnameDefaulting(t *testing.T) {
	t.Setenv("VAULTIC_HOST", "testhost")

	tests := []struct {
		name string
		args []string
		want []string
	}{
		{
			name: "env default when flag not set",
			args: nil,
			want: []string{"testhost"},
		},
		{
			name: "flag overrides env",
			args: []string{"--host", "flaghost"},
			want: []string{"flaghost"},
		},
		{
			name: "empty flag clears env",
			args: []string{"--host", ""},
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			set := pflag.NewFlagSet(tt.name, pflag.ContinueOnError)
			options := forgetOptions{}
			options.AddFlags(set)
			err := set.Parse(tt.args)
			rtest.Assert(t, err == nil, "expected no error for input")
			finalizeSnapshotFilter(&options.SnapshotFilter)
			rtest.Equals(t, tt.want, options.Hosts)
		})
	}
}

func TestValidateForgetPolicy(t *testing.T) {
	tests := []struct {
		name    string
		options forgetOptions
		wantErr string
	}{
		{name: "retention policy", options: forgetOptions{Last: 1}},
		{name: "missing policy", wantErr: "Fatal: no policy was specified, no snapshots will be removed"},
		{
			name:    "unsafe without filter",
			options: forgetOptions{UnsafeAllowRemoveAll: true},
			wantErr: "Fatal: --unsafe-allow-remove-all is not allowed unless a snapshot filter option is specified",
		},
		{
			name: "unsafe with filter",
			options: forgetOptions{
				UnsafeAllowRemoveAll: true,
				SnapshotFilter:       data.SnapshotFilter{Hosts: []string{"host"}},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateForgetPolicy(test.options, forgetPolicy(test.options))
			if test.wantErr == "" {
				rtest.OK(t, err)
				return
			}
			rtest.Assert(t, err != nil, "expected error %q", test.wantErr)
			rtest.Equals(t, test.wantErr, err.Error())
		})
	}
}
