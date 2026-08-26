package main

import (
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/itchyny/gojq"
	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/errors"
	"github.com/spf13/pflag"
)

// This file holds the pflag.Value adapters that populate the extended fields
// of data.SnapshotFilter from the rustic-compatible --filter-* flags. They are
// registered once for every snapshot-selecting command via
// initExtendedSnapshotFilter.

// stringListFlag collects repeated comma-separated lists into [][]string.
type stringListFlag struct {
	target *[][]string
}

type tagListFlag struct {
	target *data.TagLists
}

func (f *tagListFlag) String() string { return "" }
func (f *tagListFlag) Type() string   { return "list" }
func (f *tagListFlag) Set(s string) error {
	if s == "" {
		return errors.New("empty list")
	}
	list := data.TagList(strings.Split(s, ","))
	*f.target = append(*f.target, list)
	return nil
}

type jqFlag struct {
	target *string
}

func (f *jqFlag) String() string { return *f.target }
func (f *jqFlag) Type() string   { return "jq" }
func (f *jqFlag) Set(s string) error {
	if _, err := gojq.Parse(s); err != nil {
		return errors.Errorf("invalid jq expression: %v", err)
	}
	*f.target = s
	return nil
}

func (f *stringListFlag) String() string { return "" }
func (f *stringListFlag) Type() string   { return "list" }
func (f *stringListFlag) Set(s string) error {
	if s == "" {
		return errors.New("empty list")
	}
	*f.target = append(*f.target, strings.Split(s, ","))
	return nil
}

// timeFlag parses a timestamp for --filter-after / --filter-before.
type timeFlag struct {
	target *time.Time
}

func (f *timeFlag) String() string {
	if f.target == nil || f.target.IsZero() {
		return ""
	}
	return f.target.Format(time.RFC3339)
}
func (f *timeFlag) Type() string { return "time" }
func (f *timeFlag) Set(s string) error {
	t, err := parseFilterTime(s)
	if err != nil {
		return err
	}
	*f.target = t
	return nil
}

// parseFilterTime accepts RFC3339, the vaultic time format, or a plain date.
func parseFilterTime(s string) (time.Time, error) {
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05", "2006-01-02"} {
		if t, err := time.ParseInLocation(layout, s, time.Local); err == nil {
			return t, nil
		}
	}
	return time.Time{}, errors.Errorf("invalid time %q (use RFC3339 or '2006-01-02[ 15:04:05]')", s)
}

// sizeRangeFlag parses rustic's "min..max" sizes. The legacy "min:max" form
// remains accepted, as do open endpoints and decimal/IEC units.
type sizeRangeFlag struct {
	min, max *uint64
}

func (f *sizeRangeFlag) String() string { return "" }
func (f *sizeRangeFlag) Type() string   { return "size" }
func (f *sizeRangeFlag) Set(s string) error {
	lo, hi := s, ""
	if i := strings.Index(s, ".."); i >= 0 {
		lo, hi = s[:i], s[i+2:]
	} else if i := strings.Index(s, ":"); i >= 0 {
		lo, hi = s[:i], s[i+1:]
	}
	if strings.TrimSpace(lo) != "" {
		loV, err := parseFilterSize(lo)
		if err != nil {
			return err
		}
		*f.min = loV
	} else {
		*f.min = 0
	}
	if strings.TrimSpace(hi) != "" {
		hiV, err := parseFilterSize(hi)
		if err != nil {
			return err
		}
		*f.max = hiV
	} else {
		*f.max = 0
	}
	return nil
}

// parseFilterSize parses a byte size with an optional k/m/g/t suffix (base 1024).
func parseFilterSize(s string) (uint64, error) {
	v, err := humanize.ParseBytes(strings.TrimSpace(s))
	if err != nil {
		return 0, errors.Errorf("invalid size %q", s)
	}
	return v, nil
}

// initExtendedSnapshotFilter registers the rustic-compatible --filter-* flags.
// They are additive to the classic --host/--tag/--path flags.
func initExtendedSnapshotFilter(flags *pflag.FlagSet, filt *data.SnapshotFilter, withLast bool) {
	flags.StringArrayVar(&filt.FilterHosts, "filter-host", nil, "only consider snapshots with this `host` (can be specified multiple times)")
	flags.StringArrayVar(&filt.Labels, "filter-label", nil, "only consider snapshots with this `label` (can be specified multiple times)")
	flags.Var(&stringListFlag{target: &filt.FilterPaths}, "filter-paths", "only consider snapshots containing this `path[,path,...]` list (can be specified multiple times)")
	flags.Var(&stringListFlag{target: &filt.PathsExact}, "filter-paths-exact", "only consider snapshots whose paths exactly match this `path[,path,...]` list (can be specified multiple times)")
	flags.Var(&tagListFlag{target: &filt.FilterTags}, "filter-tags", "only consider snapshots containing this `tag[,tag,...]` list (can be specified multiple times)")
	flags.Var(&stringListFlag{target: &filt.TagsExact}, "filter-tags-exact", "only consider snapshots whose tags exactly match this `tag[,tag,...]` list (can be specified multiple times)")
	flags.Var(&timeFlag{target: &filt.After}, "filter-after", "only consider snapshots taken at or after `time` (e.g. 2024-01-01 or RFC3339)")
	flags.Var(&timeFlag{target: &filt.TimestampLimit}, "filter-before", "only consider snapshots taken before `time` (e.g. 2024-01-01 or RFC3339)")
	flags.Var(&sizeRangeFlag{min: &filt.SizeMin, max: &filt.SizeMax}, "filter-size", "only consider snapshots with a total size within `min..max` (rustic size units accepted)")
	flags.Var(&sizeRangeFlag{min: &filt.SizeAddedMin, max: &filt.SizeAddedMax}, "filter-size-added", "only consider snapshots adding within `min..max` to the repository")
	if withLast {
		flags.IntVar(&filt.FilterLast, "filter-last", 0, "only consider the newest `n` matching snapshots")
	}
	flags.Var(&jqFlag{target: &filt.FilterJQ}, "filter-jq", "only consider snapshots matching this `jq` expression (must return boolean)")
}
