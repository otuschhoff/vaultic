package data

import (
	"encoding/json"
	"testing"
	"time"

	rtest "github.com/otuschhoff/vaultic/internal/test"
)

func TestDeleteOptionJSON(t *testing.T) {
	// Never round-trip
	d := DeleteOption{Never: true}
	data, err := json.Marshal(d)
	rtest.OK(t, err)
	rtest.Equals(t, `"Never"`, string(data))

	var d2 DeleteOption
	rtest.OK(t, json.Unmarshal(data, &d2))
	rtest.Assert(t, d2.Never, "Never not round-tripped")

	// After round-trip
	ts := time.Date(2026, 9, 5, 15, 2, 26, 13173, time.UTC)
	d = DeleteOption{After: &ts}
	data, err = json.Marshal(d)
	rtest.OK(t, err)
	var m map[string]string
	rtest.OK(t, json.Unmarshal(data, &m))
	if _, ok := m["After"]; !ok {
		t.Fatalf("expected After key, got %s", data)
	}

	var d3 DeleteOption
	rtest.OK(t, json.Unmarshal(data, &d3))
	rtest.Assert(t, d3.After != nil && d3.After.Equal(ts), "After not round-tripped: %v", d3)
}

// TestDeleteOptionRusticCompat verifies the exact wire format rustic produces.
func TestDeleteOptionRusticCompat(t *testing.T) {
	// as produced by rustic for --delete-never
	var never DeleteOption
	rtest.OK(t, json.Unmarshal([]byte(`"Never"`), &never))
	rtest.Assert(t, never.Never, "rustic Never not parsed")

	// as produced by rustic for --delete-after (jiff Zoned with IANA suffix)
	var after DeleteOption
	rtest.OK(t, json.Unmarshal([]byte(`{"After": "2026-09-05T15:02:26.013173+02:00[Europe/Paris]"}`), &after))
	rtest.Assert(t, after.After != nil, "rustic After not parsed")
	rtest.Equals(t, 2026, after.After.Year())
	rtest.Equals(t, time.Month(9), after.After.Month())
	rtest.Equals(t, 5, after.After.Day())
}

func TestDeleteOptionMustKeepDelete(t *testing.T) {
	now := time.Now()
	future := now.Add(24 * time.Hour)
	past := now.Add(-24 * time.Hour)

	var unset *DeleteOption
	rtest.Assert(t, !unset.MustKeep(now), "unset must not keep")

	never := &DeleteOption{Never: true}
	rtest.Assert(t, never.MustKeep(now), "Never must keep")

	futureAfter := &DeleteOption{After: &future}
	rtest.Assert(t, futureAfter.MustKeep(now), "future After must keep")
	rtest.Assert(t, !futureAfter.MustDelete(now), "future After must not delete")

	pastAfter := &DeleteOption{After: &past}
	rtest.Assert(t, !pastAfter.MustKeep(now), "past After must not keep")
	rtest.Assert(t, pastAfter.MustDelete(now), "past After must delete")
}
