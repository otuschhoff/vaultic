package indexcmd

import (
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"os"
	"strings"
	"testing"
	"time"
)

func TestParseVerificationOptionsCurrentStatusAndSizes(t *testing.T) {
	selection, _, err := parseVerificationOptions(verifyStorageOptions{CurrentStatus: true, Level: "checksum", MinSize: 100, MaxSize: 200, Concurrency: 1})
	if err != nil || selection.MinSize == nil || *selection.MinSize != 100 || selection.MaxSize == nil || *selection.MaxSize != 200 {
		t.Fatalf("parsed selection = %+v, %v", selection, err)
	}
	if _, _, err := parseVerificationOptions(verifyStorageOptions{CurrentStatus: true, All: true, Level: "header", Concurrency: 1}); err == nil ||
		!strings.Contains(err.Error(), "sampling mode") {
		t.Fatalf("current status accepted sampling mode: %v", err)
	}
	if _, _, err := parseVerificationOptions(verifyStorageOptions{All: true, Level: "header", MinSize: 201, MaxSize: 200, Concurrency: 1}); err == nil ||
		!strings.Contains(err.Error(), "min-size") {
		t.Fatalf("invalid size range accepted: %v", err)
	}
	future := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	if _, _, err := parseVerificationOptions(verifyStorageOptions{All: true, Level: "header", NotVerifiedSince: future, Concurrency: 1}); err == nil ||
		!strings.Contains(err.Error(), "future") {
		t.Fatalf("future freshness cutoff accepted: %v", err)
	}
}

func TestLoadGDPRSigningKeyPKCS8(t *testing.T) {
	seed := sha256.Sum256([]byte("stable operator signing identity"))
	want := ed25519.NewKeyFromSeed(seed[:])
	der, err := x509.MarshalPKCS8PrivateKey(want)
	if err != nil {
		t.Fatal(err)
	}
	path := t.TempDir() + "/gdpr-signing.pem"
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := loadGDPRSigningKey(path)
	if err != nil || !got.Equal(want) {
		t.Fatalf("loaded signing key mismatch: %v", err)
	}
	if _, err := loadGDPRSigningKey(""); err == nil {
		t.Fatal("missing signing key was accepted")
	}
}
