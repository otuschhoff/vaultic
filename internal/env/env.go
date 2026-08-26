// Package env provides helpers to read vaultic's environment variables.
//
// All vaultic-specific variables are read under the VAULTIC_ prefix. For
// backwards compatibility with restic, a variable that is unset (or empty)
// falls back to the legacy RESTIC_ prefixed name. For example, Get("PASSWORD")
// returns $VAULTIC_PASSWORD if set, otherwise $RESTIC_PASSWORD.
package env

import "os"

const (
	// PrimaryPrefix is the prefix for vaultic's own environment variables.
	PrimaryPrefix = "VAULTIC_"
	// LegacyPrefix is the prefix used by restic, still accepted as a fallback.
	LegacyPrefix = "RESTIC_"
)

// Lookup returns the value of the environment variable VAULTIC_<name>. If
// that variable is unset or empty, it falls back to RESTIC_<name>. Empty
// values count as unset. The boolean result reports whether a non-empty
// value was found.
func Lookup(name string) (string, bool) {
	if v := os.Getenv(PrimaryPrefix + name); v != "" {
		return v, true
	}
	if v := os.Getenv(LegacyPrefix + name); v != "" {
		return v, true
	}
	return "", false
}

// Get returns the value of the environment variable VAULTIC_<name> or, as a
// fallback, RESTIC_<name>. It returns "" if neither is set to a non-empty
// value.
func Get(name string) string {
	v, _ := Lookup(name)
	return v
}

// LookupPrefixed is Lookup for variables with an additional infix prefix,
// e.g. "FROM_" for secondary repositories: it resolves
// VAULTIC_FROM_<name> with fallback to RESTIC_FROM_<name>.
func LookupPrefixed(prefix, name string) (string, bool) {
	if v := os.Getenv(prefix + PrimaryPrefix + name); v != "" {
		return v, true
	}
	if v := os.Getenv(prefix + LegacyPrefix + name); v != "" {
		return v, true
	}
	return "", false
}

// GetPrefixed is Get for variables with an additional infix prefix.
func GetPrefixed(prefix, name string) string {
	v, _ := LookupPrefixed(prefix, name)
	return v
}
