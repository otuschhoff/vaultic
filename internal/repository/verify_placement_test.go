package repository

import (
	"context"
	"testing"

	"github.com/otuschhoff/vaultic/internal/backend"
	backendtest "github.com/otuschhoff/vaultic/internal/backend/test"
	"github.com/otuschhoff/vaultic/internal/index/schema"
)

func TestVerifyPackPlacementLevelsAndCorruptionClassification(t *testing.T) {
	ctx := context.Background()
	repo, _, _, packID, _ := promotionTestRepository(t)
	model, err := repo.PlacementModel()
	if err != nil {
		t.Fatal(err)
	}
	backendHash := model.Backends[0].Hash
	for _, level := range []schema.VerificationLevel{schema.VerificationHeader, schema.VerificationChecksum, schema.VerificationFull} {
		if err := repo.VerifyPackPlacement(ctx, packID, backendHash, level); err != nil {
			t.Fatalf("level %v failed for valid pack: %v", level, err)
		}
	}
	handle := backend.Handle{Type: backend.PackFile, Name: packID.String()}
	original, err := backendtest.LoadAll(ctx, repo.be, handle)
	if err != nil {
		t.Fatal(err)
	}
	replace := func(data []byte) {
		t.Helper()
		if err := repo.be.Remove(ctx, handle); err != nil {
			t.Fatal(err)
		}
		if err := repo.be.Save(ctx, handle, backend.NewByteReader(data, repo.be.Hasher())); err != nil {
			t.Fatal(err)
		}
	}
	payloadDamage := append([]byte(nil), original...)
	payloadDamage[0] ^= 1
	replace(payloadDamage)
	if err := repo.VerifyPackPlacement(ctx, packID, backendHash, schema.VerificationHeader); err != nil {
		t.Fatalf("payload damage unexpectedly broke header verification: %v", err)
	}
	assertPlacementClassification(t, repo.VerifyPackPlacement(ctx, packID, backendHash, schema.VerificationChecksum), schema.VerificationChecksumMismatch)

	headerDamage := append([]byte(nil), original...)
	headerDamage[len(headerDamage)-1] ^= 1
	replace(headerDamage)
	assertPlacementClassification(t, repo.VerifyPackPlacement(ctx, packID, backendHash, schema.VerificationHeader), schema.VerificationHeaderAuthentication)
}

func assertPlacementClassification(t *testing.T, err error, expected schema.VerificationClassification) {
	t.Helper()
	classified, ok := err.(interface {
		VerificationClassification() (schema.VerificationClassification, string, string)
	})
	if !ok {
		t.Fatalf("error %v has no verification classification", err)
	}
	classification, _, _ := classified.VerificationClassification()
	if classification != expected {
		t.Fatalf("classification = %v, want %v: %v", classification, expected, err)
	}
}
