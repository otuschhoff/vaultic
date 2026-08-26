package repository

import (
	"testing"

	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func TestPackSizerMatchesRusticGrowth(t *testing.T) {
	const mib = uint64(1024 * 1024)

	tests := []struct {
		name        string
		defaultSize uint
		growFactor  uint
		sizeLimit   uint
		currentSize uint64
		want        uint
	}{
		{"default", 4 * uint(mib), 32, 0, 0, 4 * uint(mib)},
		{"one gibibyte", 4 * uint(mib), 32, 0, 1 * uint64(1024*mib), 5 * uint(mib)},
		{"explicit limit", 4 * uint(mib), 32, 4 * uint(mib), 1 * uint64(1024*mib), 4 * uint(mib)},
		{"zero growth", 4 * uint(mib), 0, 0, 1 * uint64(1024*mib), 4 * uint(mib)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sizer := newPackSizer(test.defaultSize, test.growFactor, test.sizeLimit, test.currentSize)
			if got := sizer.target(); got != test.want {
				t.Fatalf("target pack size = %d, want %d", got, test.want)
			}
		})
	}
}

func TestIntegerSqrt(t *testing.T) {
	for value, want := range map[uint64]uint64{
		0:          0,
		1:          1,
		2:          1,
		3:          1,
		4:          2,
		15:         3,
		16:         4,
		17:         4,
		^uint64(0): ^uint64(0) >> 32,
	} {
		if got := integerSqrt(value); got != want {
			t.Fatalf("integerSqrt(%d) = %d, want %d", value, got, want)
		}
	}
}

func TestRepositoryPackSizingUsesRusticDefaults(t *testing.T) {
	repo := TestRepository(t)

	treeSize, treeLimit, treeGrow := repo.packSizing(vaultic.TreeBlob)
	dataSize, dataLimit, dataGrow := repo.packSizing(vaultic.DataBlob)
	if treeSize != vaultic.DefaultTreePackSize || treeLimit != 0 || treeGrow != vaultic.DefaultPackGrowFactor {
		t.Fatalf("tree sizing = (%d, %d, %d), want (%d, 0, %d)", treeSize, treeLimit, treeGrow, vaultic.DefaultTreePackSize, vaultic.DefaultPackGrowFactor)
	}
	if dataSize != vaultic.DefaultDataPackSize || dataLimit != 0 || dataGrow != vaultic.DefaultPackGrowFactor {
		t.Fatalf("data sizing = (%d, %d, %d), want (%d, 0, %d)", dataSize, dataLimit, dataGrow, vaultic.DefaultDataPackSize, vaultic.DefaultPackGrowFactor)
	}
}
