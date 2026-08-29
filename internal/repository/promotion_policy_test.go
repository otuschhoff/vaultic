package repository

import (
	"math"
	"testing"

	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func TestPromotionEligibilityUsesShortestRetainedBlobHorizon(t *testing.T) {
	packID := vaultic.NewRandomID()
	short, long, dead := vaultic.NewRandomID(), vaultic.NewRandomID(), vaultic.NewRandomID()
	records := promotionEligibilityRecords(
		map[vaultic.ID][]vaultic.ID{packID: {short, long, dead}},
		map[vaultic.ID]struct{}{short: {}, long: {}},
		map[vaultic.ID]int64{short: 200, long: math.MaxInt64},
		100,
	)
	record, found := records[packID]
	if !found || record.SurvivalUntil != 200 || record.Indefinite || record.EvaluatedAt != 100 {
		t.Fatalf("eligibility = %#v, found=%v", record, found)
	}
}

func TestPromotionEligibilityOmitsUnretainedPack(t *testing.T) {
	packID, dead := vaultic.NewRandomID(), vaultic.NewRandomID()
	records := promotionEligibilityRecords(map[vaultic.ID][]vaultic.ID{packID: {dead}}, nil, nil, 100)
	if _, found := records[packID]; found {
		t.Fatal("unretained pack received promotion eligibility")
	}
}

func TestPromotionEligibilityUnknownLiveBlobVetoesPack(t *testing.T) {
	packID := vaultic.NewRandomID()
	known, unknown := vaultic.NewRandomID(), vaultic.NewRandomID()
	records := promotionEligibilityRecords(
		map[vaultic.ID][]vaultic.ID{packID: {known, unknown}},
		map[vaultic.ID]struct{}{known: {}, unknown: {}},
		map[vaultic.ID]int64{known: math.MaxInt64}, 100,
	)
	if _, found := records[packID]; found {
		t.Fatal("pack with unknown live blob received promotion eligibility")
	}
}
