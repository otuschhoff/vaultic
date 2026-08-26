package data

import (
	"encoding/json"
	"fmt"
	"time"
)

// DeleteOption marks a snapshot as delete-protected. The JSON serialization
// mirrors rustic's DeleteOption (an externally tagged serde enum):
//
//	not set     -> field omitted
//	never       -> "Never"                          (a bare string)
//	after <t>   -> {"After": "<RFC3339 timestamp>"}  (an object)
//
// Note: rustic serializes After using jiff's Zoned (RFC 3339 with an
// "[IANA/Timezone]" suffix). Go's time.Time parses the plain RFC 3339 part; a
// bracketed timezone suffix is tolerated (stripped) on read.
type DeleteOption struct {
	// Never protects the snapshot from deletion indefinitely.
	Never bool
	// After, when non-nil and in the future, protects the snapshot from
	// deletion until that time.
	After *time.Time
}

// MarshalJSON implements json.Marshaler. The After timestamp is written in
// jiff's Zoned format (RFC 3339 with an "[IANA/Zone]" suffix) so that rustic
// can parse it.
func (d DeleteOption) MarshalJSON() ([]byte, error) {
	if d.Never {
		return json.Marshal("Never")
	}
	if d.After != nil {
		return json.Marshal(struct {
			After string `json:"After"`
		}{After: formatZoned(*d.After)})
	}
	return nil, fmt.Errorf("empty DeleteOption cannot be serialized")
}

// formatZoned formats t in the jiff Zoned layout used by rustic:
// RFC 3339 followed by the IANA time zone in square brackets, e.g.
// "2026-09-05T15:02:26.013173+02:00[Europe/Paris]". Falls back to UTC.
func formatZoned(t time.Time) string {
	loc := t.Location()
	name := loc.String()
	if name == "" || name == "Local" {
		name = "UTC"
		loc = time.UTC
	}
	return t.In(loc).Format("2006-01-02T15:04:05.999999999Z07:00") + "[" + name + "]"
}

// UnmarshalJSON implements json.Unmarshaler.
func (d *DeleteOption) UnmarshalJSON(data []byte) error {
	// bare string variant: "Never"
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		if s != "Never" {
			return fmt.Errorf("invalid delete option %q", s)
		}
		*d = DeleteOption{Never: true}
		return nil
	}

	// object variant: {"After": "..."}
	var obj struct {
		After string `json:"After"`
	}
	if err := json.Unmarshal(data, &obj); err != nil {
		return fmt.Errorf("invalid delete option: %w", err)
	}
	t, err := parseDeleteAfter(obj.After)
	if err != nil {
		return fmt.Errorf("invalid delete-after timestamp %q: %w", obj.After, err)
	}
	*d = DeleteOption{After: &t}
	return nil
}

// parseDeleteAfter parses an RFC 3339 timestamp, tolerating a trailing jiff
// "[IANA/Timezone]" suffix as written by rustic.
func parseDeleteAfter(s string) (time.Time, error) {
	if i := indexByte(s, '['); i >= 0 {
		s = s[:i]
	}
	return time.Parse(time.RFC3339Nano, s)
}

func indexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}

// IsSet reports whether any delete protection is set.
func (d *DeleteOption) IsSet() bool {
	return d != nil && (d.Never || d.After != nil)
}

// MustKeep reports whether the snapshot must be kept at time now, i.e. it is
// protected from deletion (Never, or After a future time).
func (d *DeleteOption) MustKeep(now time.Time) bool {
	if !d.IsSet() {
		return false
	}
	if d.Never {
		return true
	}
	return d.After != nil && !d.After.Before(now)
}

// MustDelete reports whether the snapshot must be deleted now, i.e. its
// After timestamp has passed.
func (d *DeleteOption) MustDelete(now time.Time) bool {
	return d != nil && d.After != nil && d.After.Before(now)
}

// String describes the delete protection.
func (d *DeleteOption) String() string {
	switch {
	case d == nil || !d.IsSet():
		return ""
	case d.Never:
		return "delete: never"
	default:
		return fmt.Sprintf("delete: not before %s", d.After.Format(time.RFC3339))
	}
}
