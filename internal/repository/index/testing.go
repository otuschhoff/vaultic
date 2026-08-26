package index

import (
	"testing"

	"github.com/vaultic/vaultic/internal/test"
	"github.com/vaultic/vaultic/internal/vaultic"
)

func TestMergeIndex(t testing.TB, mi *MasterIndex) ([]*Index, int, vaultic.IDSet) {
	finalIndexes := mi.finalizeNotFinalIndexes()
	ids := vaultic.NewIDSet()
	for _, idx := range finalIndexes {
		id := vaultic.NewRandomID()
		ids.Insert(id)
		test.OK(t, idx.SetID(id))
	}

	test.OK(t, mi.MergeFinalIndexes())
	return finalIndexes, len(mi.idx), ids
}
