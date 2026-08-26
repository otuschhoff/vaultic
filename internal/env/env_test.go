package env

import "testing"

func TestGetPrimary(t *testing.T) {
	t.Setenv("VAULTIC_FOO", "primary")
	if got := Get("FOO"); got != "primary" {
		t.Fatalf("expected primary value, got %q", got)
	}
}

func TestGetLegacyFallback(t *testing.T) {
	t.Setenv("RESTIC_FOO", "legacy")
	if got := Get("FOO"); got != "legacy" {
		t.Fatalf("expected legacy fallback value, got %q", got)
	}
}

func TestGetPrimaryWins(t *testing.T) {
	t.Setenv("VAULTIC_FOO", "primary")
	t.Setenv("RESTIC_FOO", "legacy")
	if got := Get("FOO"); got != "primary" {
		t.Fatalf("expected primary to win, got %q", got)
	}
}

func TestGetEmptyCountsAsUnset(t *testing.T) {
	t.Setenv("VAULTIC_FOO", "")
	t.Setenv("RESTIC_FOO", "legacy")
	if got := Get("FOO"); got != "legacy" {
		t.Fatalf("expected empty primary to fall back to legacy, got %q", got)
	}
}

func TestGetUnset(t *testing.T) {
	if got := Get("DEFINITELY_UNSET_VARIABLE"); got != "" {
		t.Fatalf("expected empty string, got %q", got)
	}
	if _, ok := Lookup("DEFINITELY_UNSET_VARIABLE"); ok {
		t.Fatal("expected lookup to report unset variable")
	}
}

func TestGetPrefixed(t *testing.T) {
	t.Setenv("RESTIC_FROM_REPOSITORY", "legacy")
	if got := GetPrefixed("", "FROM_REPOSITORY"); got != "legacy" {
		t.Fatalf("expected legacy fallback, got %q", got)
	}
	t.Setenv("VAULTIC_FROM_REPOSITORY", "primary")
	if got := GetPrefixed("", "FROM_REPOSITORY"); got != "primary" {
		t.Fatalf("expected primary, got %q", got)
	}
}
