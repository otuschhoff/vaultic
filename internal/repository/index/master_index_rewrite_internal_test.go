package index

import (
	"testing"

	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func TestCloneRewriteExcludes(t *testing.T) {
	first := vaultic.NewRandomID()
	second := vaultic.NewRandomID()
	excludes := vaultic.NewIDSet(first)

	cloned := cloneRewriteExcludes(excludes)
	excludes.Insert(second)
	if cloned.Has(second) {
		t.Fatal("clone changed when source set was mutated")
	}

	if empty := cloneRewriteExcludes(nil); empty == nil || len(empty) != 0 {
		t.Fatalf("nil excludes normalized to %#v, want non-nil empty set", empty)
	}
}

func TestRewriteIndexIDsPreservesExplicitSet(t *testing.T) {
	master := NewMasterIndex()
	first := vaultic.NewRandomID()
	second := vaultic.NewRandomID()
	explicit := vaultic.NewIDSet(first)

	selected := rewriteIndexIDs(master, explicit)
	selected.Insert(second)
	if !explicit.Has(second) {
		t.Fatal("explicit index set was unexpectedly copied")
	}
}
