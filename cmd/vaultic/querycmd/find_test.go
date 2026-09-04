package querycmd

import (
	"testing"

	"github.com/otuschhoff/vaultic/internal/data"
	rtest "github.com/otuschhoff/vaultic/internal/test"
	"github.com/spf13/pflag"
)

func TestSnapshotFilter(t *testing.T) {
	for _, test := range []struct {
		name     string
		args     []string
		expected []string
		env      string
	}{
		{
			"no value",
			[]string{},
			nil,
			"",
		},
		{
			"args only",
			[]string{"--host", "abc"},
			[]string{"abc"},
			"",
		},
		{
			"env default",
			[]string{},
			[]string{"def"},
			"def",
		},
		{
			"both",
			[]string{"--host", "abc"},
			[]string{"abc"},
			"def",
		},
		{
			"env set, empty flag overrides",
			[]string{"--host", ""},
			nil, // empty host filter means all hosts
			"envhost",
		},
		{
			"env set, multiple flags override",
			[]string{"--host", "host1", "--host", "host2"},
			[]string{"host1", "host2"},
			"envhost",
		},
		{
			"env set, multiple hosts including empty",
			[]string{"--host", "host1", "--host", ""},
			[]string{"host1", ""},
			"envhost",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("VAULTIC_HOST", test.env)

			for _, mode := range []bool{false, true} {
				set := pflag.NewFlagSet("test", pflag.PanicOnError)
				flt := &data.SnapshotFilter{}
				if mode {
					initMultiSnapshotFilter(set, flt, false)
				} else {
					initSingleSnapshotFilter(set, flt)
				}
				err := set.Parse(test.args)
				rtest.OK(t, err)

				// Apply the finalization logic to handle env defaults
				finalizeSnapshotFilter(flt)

				rtest.Equals(t, test.expected, flt.Hosts, "unexpected hosts")
			}
		})
	}
}

func TestRusticSnapshotFilterFlags(t *testing.T) {
	set := pflag.NewFlagSet("test", pflag.ContinueOnError)
	filter := &data.SnapshotFilter{}
	initMultiSnapshotFilter(set, filter, true)
	err := set.Parse([]string{
		"--filter-host", "host-a",
		"--filter-label", "daily",
		"--filter-paths", "/a,/b",
		"--filter-paths-exact", "/a,/b",
		"--filter-tags", "one,two",
		"--filter-tags-exact", "one,two",
		"--filter-after", "2024-01-01",
		"--filter-before", "2024-02-01",
		"--filter-size", "1 MB .. 2 GiB",
		"--filter-size-added", "3k..4k",
		"--filter-last", "2",
		"--filter-jq", `.label == "daily"`,
	})
	rtest.OK(t, err)
	rtest.Equals(t, []string{"host-a"}, filter.FilterHosts)
	rtest.Equals(t, []string{"daily"}, filter.Labels)
	rtest.Equals(t, [][]string{{"/a", "/b"}}, filter.FilterPaths)
	rtest.Equals(t, data.TagLists{{"one", "two"}}, filter.FilterTags)
	rtest.Equals(t, uint64(1_000_000), filter.SizeMin)
	rtest.Equals(t, uint64(2<<30), filter.SizeMax)
	rtest.Equals(t, uint64(3000), filter.SizeAddedMin)
	rtest.Equals(t, uint64(4000), filter.SizeAddedMax)
	rtest.Equals(t, 2, filter.FilterLast)
	rtest.Equals(t, `.label == "daily"`, filter.FilterJQ)

	invalid := pflag.NewFlagSet("test", pflag.ContinueOnError)
	invalidFilter := &data.SnapshotFilter{}
	initMultiSnapshotFilter(invalid, invalidFilter, true)
	err = invalid.Parse([]string{"--filter-jq", ".label =="})
	rtest.Assert(t, err != nil, "expected invalid jq expression to be rejected")
}

func TestSizeRangeFlagOpenBoundsReset(t *testing.T) {
	set := pflag.NewFlagSet("test", pflag.ContinueOnError)
	filter := &data.SnapshotFilter{}
	initMultiSnapshotFilter(set, filter, true)
	rtest.OK(t, set.Parse([]string{"--filter-size", "1MB..2MB", "--filter-size", "3MB.."}))
	rtest.Equals(t, uint64(3_000_000), filter.SizeMin)
	rtest.Equals(t, uint64(0), filter.SizeMax)
}
