package configfile

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/pflag"
)

func TestLoadIncludesAndFlagPrecedence(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "base.toml")
	child := filepath.Join(dir, "child.toml")
	if err := os.WriteFile(base, []byte("[global]\nverbose = 1\n[backup]\nlabel = 'base'\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(child, []byte("use-profiles = ['base.toml']\n[global]\nverbose = 2\n[backup]\nlabel = 'child'\n[[backup.snapshots]]\nname = 'home'\nsources = ['/home/me']\n"), 0600); err != nil {
		t.Fatal(err)
	}

	p, err := Load([]string{child})
	if err != nil {
		t.Fatal(err)
	}
	if got := p.Sections["global"]["verbose"]; got != int64(2) {
		t.Fatalf("global.verbose = %v, want 2", got)
	}
	if got := p.Sections["backup"]["label"]; got != "child" {
		t.Fatalf("backup.label = %v, want child", got)
	}
	if len(p.Snapshots) != 1 || p.Snapshots[0].Name != "home" {
		t.Fatalf("unexpected jobs: %#v", p.Snapshots)
	}

	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	var label string
	flags.StringVar(&label, "label", "", "")
	if err := p.ApplyFlags("backup", flags, func(string) bool { return false }); err != nil {
		t.Fatal(err)
	}
	if label != "child" {
		t.Fatalf("label = %q, want child", label)
	}
	if err := flags.Set("label", "cli"); err != nil {
		t.Fatal(err)
	}
	if err := p.ApplyFlags("backup", flags, func(string) bool { return false }); err != nil {
		t.Fatal(err)
	}
	if label != "cli" {
		t.Fatalf("CLI value overwritten: %q", label)
	}
}

func TestLoadRejectsIncludeCycle(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "a.toml")
	b := filepath.Join(dir, "b.toml")
	if err := os.WriteFile(a, []byte("use-profiles = ['b.toml']\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(b, []byte("use-profiles = ['a.toml']\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load([]string{a}); err == nil {
		t.Fatal("Load succeeded for include cycle")
	}
}
