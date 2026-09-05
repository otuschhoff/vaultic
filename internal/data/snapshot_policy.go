package data

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/otuschhoff/vaultic/internal/debug"
)

// ExpirePolicy configures which snapshots should be automatically removed.
type ExpirePolicy struct {
	Last                int       // keep the last n snapshots
	Minutely            int       // keep the last n minutely snapshots
	Hourly              int       // keep the last n hourly snapshots
	Daily               int       // keep the last n daily snapshots
	Weekly              int       // keep the last n weekly snapshots
	Monthly             int       // keep the last n monthly snapshots
	QuarterYearly       int       // keep the last n quarterly snapshots
	HalfYearly          int       // keep the last n half-yearly snapshots
	Yearly              int       // keep the last n yearly snapshots
	Within              Duration  // keep snapshots made within this duration
	WithinMinutely      Duration  // keep minutely snapshots made within this duration
	WithinHourly        Duration  // keep hourly snapshots made within this duration
	WithinDaily         Duration  // keep daily snapshots made within this duration
	WithinWeekly        Duration  // keep weekly snapshots made within this duration
	WithinMonthly       Duration  // keep monthly snapshots made within this duration
	WithinQuarterYearly Duration  // keep quarterly snapshots made within this duration
	WithinHalfYearly    Duration  // keep half-yearly snapshots made within this duration
	WithinYearly        Duration  // keep yearly snapshots made within this duration
	Tags                []TagList // keep all snapshots that include at least one of the tag lists.
}

func (e ExpirePolicy) String() (s string) {
	var keeps []string
	var keepw []string

	for _, opt := range []struct {
		count int
		descr string
	}{
		{e.Last, "latest"},
		{e.Minutely, "minutely"},
		{e.Hourly, "hourly"},
		{e.Daily, "daily"},
		{e.Weekly, "weekly"},
		{e.Monthly, "monthly"},
		{e.QuarterYearly, "quarter-yearly"},
		{e.HalfYearly, "half-yearly"},
		{e.Yearly, "yearly"},
	} {
		if opt.count > 0 {
			keeps = append(keeps, fmt.Sprintf("%d %s", opt.count, opt.descr))
		} else if opt.count == -1 {
			keeps = append(keeps, fmt.Sprintf("all %s", opt.descr))
		}
	}

	if !e.WithinMinutely.Zero() {
		keepw = append(keepw, fmt.Sprintf("minutely snapshots within %v", e.WithinMinutely))
	}
	if !e.WithinHourly.Zero() {
		keepw = append(keepw, fmt.Sprintf("hourly snapshots within %v", e.WithinHourly))
	}

	if !e.WithinDaily.Zero() {
		keepw = append(keepw, fmt.Sprintf("daily snapshots within %v", e.WithinDaily))
	}

	if !e.WithinWeekly.Zero() {
		keepw = append(keepw, fmt.Sprintf("weekly snapshots within %v", e.WithinWeekly))
	}

	if !e.WithinMonthly.Zero() {
		keepw = append(keepw, fmt.Sprintf("monthly snapshots within %v", e.WithinMonthly))
	}
	if !e.WithinQuarterYearly.Zero() {
		keepw = append(keepw, fmt.Sprintf("quarter-yearly snapshots within %v", e.WithinQuarterYearly))
	}
	if !e.WithinHalfYearly.Zero() {
		keepw = append(keepw, fmt.Sprintf("half-yearly snapshots within %v", e.WithinHalfYearly))
	}

	if !e.WithinYearly.Zero() {
		keepw = append(keepw, fmt.Sprintf("yearly snapshots within %v", e.WithinYearly))
	}

	if len(keeps) > 0 {
		s = fmt.Sprintf("%s snapshots", strings.Join(keeps, ", "))
	}

	if len(keepw) > 0 {
		if s != "" {
			s += ", "
		}
		s += strings.Join(keepw, ", ")
	}

	if len(e.Tags) > 0 {
		if s != "" {
			s += " and "
		}
		s += fmt.Sprintf("all snapshots with tags %s", e.Tags)
	}

	if !e.Within.Zero() {
		if s != "" {
			s += " and "
		}
		s += fmt.Sprintf("all snapshots within %s of the newest", e.Within)
	}

	if s == "" {
		s = "remove"
	} else {
		s = "keep " + s
	}

	return s
}

// Empty returns true if no policy has been configured (all values zero).
func (e ExpirePolicy) Empty() bool {
	if len(e.Tags) != 0 {
		return false
	}

	empty := ExpirePolicy{Tags: e.Tags}
	return reflect.DeepEqual(e, empty)
}

// ymdh returns an integer in the form YYYYMMDDHH.
func ymdh(d time.Time, _ int) int {
	return d.Year()*1000000 + int(d.Month())*10000 + d.Day()*100 + d.Hour()
}

func ymdhm(d time.Time, _ int) int {
	return d.Year()*100000000 + int(d.Month())*1000000 + d.Day()*10000 + d.Hour()*100 + d.Minute()
}

// ymd returns an integer in the form YYYYMMDD.
func ymd(d time.Time, _ int) int {
	return d.Year()*10000 + int(d.Month())*100 + d.Day()
}

// yw returns an integer in the form YYYYWW, where WW is the week number.
func yw(d time.Time, _ int) int {
	year, week := d.ISOWeek()
	return year*100 + week
}

// ym returns an integer in the form YYYYMM.
func ym(d time.Time, _ int) int {
	return d.Year()*100 + int(d.Month())
}

// y returns the year of d.
func y(d time.Time, _ int) int {
	return d.Year()
}

func yq(d time.Time, _ int) int { return d.Year()*10 + (int(d.Month())-1)/3 }
func yh(d time.Time, _ int) int { return d.Year()*10 + (int(d.Month())-1)/6 }

// always returns a unique number for d.
func always(_ time.Time, nr int) int {
	return nr
}

// findLatestTimestamp returns the time stamp for the latest (newest) snapshot,
// for use with policies based on time relative to latest.
func findLatestTimestamp(list Snapshots) time.Time {
	if len(list) == 0 {
		//nolint:forbidigo // This existing panic enforces an internal invariant; new panic paths remain forbidden.
		panic("list of snapshots is empty")
	}

	var latest time.Time
	now := time.Now()
	for _, sn := range list {
		// Find the latest snapshot in the list
		// The latest snapshot must, however, not be in the future.
		if sn.Time.After(latest) && sn.Time.Before(now) {
			latest = sn.Time
		}
	}

	return latest
}

// KeepReason specifies why a particular snapshot was kept, and the counters at
// that point in the policy evaluation.
type KeepReason struct {
	Snapshot *Snapshot `json:"snapshot"`

	// description text which criteria match, e.g. "daily", "monthly"
	Matches []string `json:"matches"`

	// the counters after evaluating the current snapshot
	Counters struct {
		Last    int `json:"last,omitempty"`
		Hourly  int `json:"hourly,omitempty"`
		Daily   int `json:"daily,omitempty"`
		Weekly  int `json:"weekly,omitempty"`
		Monthly int `json:"monthly,omitempty"`
		Yearly  int `json:"yearly,omitempty"`
	} `json:"counters"`
}

type RetentionHorizon struct {
	Until      time.Time
	Indefinite bool
}

// PolicyRetentionHorizon returns the time guarantee established by the
// reasons ApplyPolicy kept a snapshot. Finite count buckets intentionally do
// not imply a duration because future snapshots can displace them immediately.
func PolicyRetentionHorizon(reason KeepReason, policy ExpirePolicy) (RetentionHorizon, bool) {
	if reason.Snapshot == nil {
		return RetentionHorizon{}, false
	}
	for _, tags := range policy.Tags {
		if reason.Snapshot.HasTags(tags) {
			return RetentionHorizon{Indefinite: true}, true
		}
	}
	match := func(prefix string) bool {
		for _, value := range reason.Matches {
			if strings.HasPrefix(value, prefix) || strings.HasPrefix(value, "oldest "+prefix) {
				return true
			}
		}
		return false
	}
	for _, bucket := range []struct {
		count  int
		reason string
	}{
		{policy.Last, "last snapshot"}, {policy.Minutely, "minutely snapshot"},
		{policy.Hourly, "hourly snapshot"}, {policy.Daily, "daily snapshot"},
		{policy.Weekly, "weekly snapshot"}, {policy.Monthly, "monthly snapshot"},
		{policy.QuarterYearly, "quarter-yearly snapshot"},
		{policy.HalfYearly, "half-yearly snapshot"}, {policy.Yearly, "yearly snapshot"},
	} {
		if bucket.count == -1 && match(bucket.reason) {
			return RetentionHorizon{Indefinite: true}, true
		}
	}
	var horizon RetentionHorizon
	setDuration := func(duration Duration) {
		until := reason.Snapshot.Time.AddDate(duration.Years, duration.Months, duration.Days).
			Add(time.Duration(duration.Hours) * time.Hour)
		if until.After(horizon.Until) {
			horizon.Until = until
		}
	}
	if !policy.Within.Zero() && match("within ") {
		setDuration(policy.Within)
	}
	for _, bucket := range []struct {
		duration Duration
		reason   string
	}{
		{policy.WithinMinutely, "minutely within"}, {policy.WithinHourly, "hourly within"},
		{policy.WithinDaily, "daily within"}, {policy.WithinWeekly, "weekly within"},
		{policy.WithinMonthly, "monthly within"},
		{policy.WithinQuarterYearly, "quarter-yearly within"},
		{policy.WithinHalfYearly, "half-yearly within"}, {policy.WithinYearly, "yearly within"},
	} {
		if !bucket.duration.Zero() && match(bucket.reason) {
			setDuration(bucket.duration)
		}
	}
	return horizon, !horizon.Until.IsZero()
}

// ApplyPolicy returns the snapshots from list that are to be kept and removed
// according to the policy p. list is sorted in the process. reasons contains
// the reasons to keep each snapshot, it is in the same order as keep.
//
//nolint:gocognit // Existing domain flow is an explicit complexity exception; new code remains gated.
func ApplyPolicy(list Snapshots, p ExpirePolicy) (keep, remove Snapshots, reasons []KeepReason) {
	// sort newest snapshots first
	sort.Stable(list)

	if len(list) == 0 {
		return list, nil, nil
	}

	// These buckets are for keeping last n snapshots of given type
	var buckets = []struct {
		Count  int
		bucker func(d time.Time, nr int) int
		Last   int
		reason string
	}{
		{p.Last, always, -1, "last snapshot"},
		{p.Hourly, ymdh, -1, "hourly snapshot"},
		{p.Daily, ymd, -1, "daily snapshot"},
		{p.Weekly, yw, -1, "weekly snapshot"},
		{p.Monthly, ym, -1, "monthly snapshot"},
		{p.Yearly, y, -1, "yearly snapshot"},
		{p.Minutely, ymdhm, -1, "minutely snapshot"},
		{p.QuarterYearly, yq, -1, "quarter-yearly snapshot"},
		{p.HalfYearly, yh, -1, "half-yearly snapshot"},
	}

	// These buckets are for keeping snapshots of given type within duration
	var bucketsWithin = []struct {
		Within Duration
		bucker func(d time.Time, nr int) int
		Last   int
		reason string
	}{
		{p.WithinHourly, ymdh, -1, "hourly within"},
		{p.WithinDaily, ymd, -1, "daily within"},
		{p.WithinWeekly, yw, -1, "weekly within"},
		{p.WithinMonthly, ym, -1, "monthly within"},
		{p.WithinYearly, y, -1, "yearly within"},
		{p.WithinMinutely, ymdhm, -1, "minutely within"},
		{p.WithinQuarterYearly, yq, -1, "quarter-yearly within"},
		{p.WithinHalfYearly, yh, -1, "half-yearly within"},
	}

	latest := findLatestTimestamp(list)

	for nr, cur := range list {
		var keepSnap bool
		var keepSnapReasons []string

		// Tags are handled specially as they are not counted.
		for _, l := range p.Tags {
			if cur.HasTags(l) {
				keepSnap = true
				keepSnapReasons = append(keepSnapReasons, fmt.Sprintf("has tags %v", l))
			}
		}

		// If the timestamp of the snapshot is within the range, then keep it.
		if !p.Within.Zero() {
			t := latest.AddDate(-p.Within.Years, -p.Within.Months, -p.Within.Days).Add(time.Hour * time.Duration(-p.Within.Hours))
			if cur.Time.After(t) {
				keepSnap = true
				keepSnapReasons = append(keepSnapReasons, fmt.Sprintf("within %v", p.Within))
			}
		}

		// Now update the other buckets and see if they have some counts left.
		for i, b := range buckets {
			// -1 means "keep all"
			//nolint:nestif // Existing domain flow is an explicit complexity exception; new code remains gated.
			if b.Count > 0 || b.Count == -1 {
				val := b.bucker(cur.Time, nr)
				// also keep the oldest snapshot if the bucket has some counts left. This maximizes the
				// the history length kept while some counts are left.
				if val != b.Last || nr == len(list)-1 {
					debug.Log("keep %v %v, bucker %v, val %v\n", cur.Time, cur.id.Str(), i, val)
					keepSnap = true
					if val == b.Last && nr == len(list)-1 {
						b.reason = fmt.Sprintf("oldest %v", b.reason)
					}
					buckets[i].Last = val
					if buckets[i].Count > 0 {
						buckets[i].Count--
					}
					keepSnapReasons = append(keepSnapReasons, b.reason)
				}
			}
		}

		// If the timestamp is within range, and the snapshot is an hourly/daily/weekly/monthly/yearly snapshot, then keep it
		for i, b := range bucketsWithin {
			//nolint:nestif // Existing domain flow is an explicit complexity exception; new code remains gated.
			if !b.Within.Zero() {
				t := latest.AddDate(-b.Within.Years, -b.Within.Months, -b.Within.Days).Add(time.Hour * time.Duration(-b.Within.Hours))

				if cur.Time.After(t) {
					val := b.bucker(cur.Time, nr)
					if val != b.Last || nr == len(list)-1 {
						debug.Log("keep %v, time %v, ID %v, bucker %v, val %v %v\n", b.reason, cur.Time, cur.id.Str(), i, val, b.Last)
						keepSnap = true
						if val == b.Last && nr == len(list)-1 {
							b.reason = fmt.Sprintf("oldest %v", b.reason)
						}
						bucketsWithin[i].Last = val
						keepSnapReasons = append(keepSnapReasons, fmt.Sprintf("%v %v", b.reason, b.Within))
					}
				}
			}
		}

		if keepSnap {
			keep = append(keep, cur)
			kr := KeepReason{
				Snapshot: cur,
				Matches:  keepSnapReasons,
			}
			kr.Counters.Last = buckets[0].Count
			kr.Counters.Hourly = buckets[1].Count
			kr.Counters.Daily = buckets[2].Count
			kr.Counters.Weekly = buckets[3].Count
			kr.Counters.Monthly = buckets[4].Count
			kr.Counters.Yearly = buckets[5].Count
			reasons = append(reasons, kr)
		} else {
			remove = append(remove, cur)
		}
	}

	return keep, remove, reasons
}
