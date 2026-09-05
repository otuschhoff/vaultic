package vaultic_test

import (
	"context"
	"errors"
	"testing"

	"github.com/otuschhoff/vaultic/internal/vaultic"
)

var samples = vaultic.IDs{
	vaultic.TestParseID("20bdc1402a6fc9b633aaffffffffffffffffffffffffffffffffffffffffffff"),
	vaultic.TestParseID("20bdc1402a6fc9b633ccd578c4a92d0f4ef1a457fa2e16c596bc73fb409d6cc0"),
	vaultic.TestParseID("20bdc1402a6fc9b633ffffffffffffffffffffffffffffffffffffffffffffff"),
	vaultic.TestParseID("20ff988befa5fc40350f00d531a767606efefe242c837aaccb80673f286be53d"),
	vaultic.TestParseID("326cb59dfe802304f96ee9b5b9af93bdee73a30f53981e5ec579aedb6f1d0f07"),
	vaultic.TestParseID("86b60b9594d1d429c4aa98fa9562082cabf53b98c7dc083abe5dae31074dd15a"),
	vaultic.TestParseID("96c8dbe225079e624b5ce509f5bd817d1453cd0a85d30d536d01b64a8669aeae"),
	vaultic.TestParseID("fa31d65b87affcd167b119e9d3d2a27b8236ca4836cb077ed3e96fcbe209b792"),
}

func TestFind(t *testing.T) {
	list := samples

	m := &ListHelper{}
	m.ListFn = func(ctx context.Context, t vaultic.FileType, fn func(id vaultic.ID, size int64) error) error {
		for _, id := range list {
			err := fn(id, 0)
			if err != nil {
				return err
			}
		}
		return nil
	}

	f, err := vaultic.Find(context.TODO(), m, vaultic.SnapshotFile, "20bdc1402a6fc9b633aa")
	if err != nil {
		t.Error(err)
	}
	expectedMatch := vaultic.TestParseID("20bdc1402a6fc9b633aaffffffffffffffffffffffffffffffffffffffffffff")
	if f != expectedMatch {
		t.Errorf("Wrong match returned want %s, got %s", expectedMatch, f)
	}

	f, err = vaultic.Find(context.TODO(), m, vaultic.SnapshotFile, "NotAPrefix")
	noIDByPrefixError := &vaultic.NoIDByPrefixError{}
	if !errors.As(err, &noIDByPrefixError) {
		t.Error("Expected no snapshots to be found.")
	}
	if !f.IsNull() {
		t.Errorf("Find should not return a match on error.")
	}

	// Try to match with a prefix longer than any ID.
	extraLengthID := samples[0].String() + "f"
	f, err = vaultic.Find(context.TODO(), m, vaultic.SnapshotFile, extraLengthID)
	noIDByPrefixError = &vaultic.NoIDByPrefixError{}
	if !errors.As(err, &noIDByPrefixError) {
		t.Errorf("Wrong error %v for no snapshots matched", err)
	}
	if !f.IsNull() {
		t.Errorf("Find should not return a match on error.")
	}

	// Use a prefix that will match the prefix of multiple Ids in `samples`.
	f, err = vaultic.Find(context.TODO(), m, vaultic.SnapshotFile, "20bdc140")
	multipleIDMatchesError := &vaultic.MultipleIDMatchesError{}
	if !errors.As(err, &multipleIDMatchesError) {
		t.Errorf("Wrong error %v for multiple snapshots", err)
	}
	if !f.IsNull() {
		t.Errorf("Find should not return a match on error.")
	}
}
