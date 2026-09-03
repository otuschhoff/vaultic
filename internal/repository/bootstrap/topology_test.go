package bootstrap

import (
	"context"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/backend/mem"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func testManifest() Manifest {
	return Manifest{
		Format: 1, RepositoryID: "repo-a", Generation: 2, CreatedAt: time.Now().UTC(), ConfigSHA256: "ab" + string(make([]byte, 0)),
		Backends: []vaultic.PlacementBackend{
			{ID: "a", Location: "s3:https://storage.example/a", FailureDomain: "site-a"},
			{ID: "b", Location: "azure:https://storage.example/b", FailureDomain: "site-b", Offsite: true},
		},
		Policy: vaultic.PlacementPolicy{MinCopies: 2, MinDomains: 2, MinOffsite: 1}, StagingBackends: []string{"a", "b"},
	}
}

func TestSealOpenAndResolveTopology(t *testing.T) {
	manifest := testManifest()
	manifest.ConfigSHA256 = "ab" + repeat("cd", 31)
	key := []byte("0123456789abcdef0123456789abcdef")
	encoded, digest, err := Seal(manifest, key)
	if err != nil {
		t.Fatal(err)
	}
	opened, openedDigest, err := Open(encoded, key, "repo-a")
	if err != nil || opened.Generation != 2 || openedDigest != digest {
		t.Fatalf("opened = %#v, %q, %v", opened, openedDigest, err)
	}
	if _, _, err := Open(encoded, []byte("abcdef0123456789abcdef0123456789"), "repo-a"); err == nil {
		t.Fatal("wrong topology key authenticated")
	}
	copy, err := Resolve([]Copy{{Seed: "a", Manifest: opened, SHA256: digest}}, Anchor{RepositoryID: "repo-a", Generation: 2, SHA256: digest})
	if err != nil || copy.Seed != "a" {
		t.Fatalf("resolve = %#v, %v", copy, err)
	}
}

func TestTopologyRejectsRollbackConflictCredentialsAndWeakPlacement(t *testing.T) {
	manifest := testManifest()
	manifest.ConfigSHA256 = repeat("ab", 32)
	manifest.Backends[0].Location = "s3:https://user:secret@storage.example/a"
	if err := manifest.Validate(); err == nil {
		t.Fatal("credential-bearing topology was accepted")
	}
	manifest = testManifest()
	manifest.ConfigSHA256 = repeat("ab", 32)
	manifest.Backends = manifest.Backends[:1]
	manifest.StagingBackends = []string{"a"}
	if err := manifest.Validate(); err == nil {
		t.Fatal("under-durable topology was accepted")
	}
	manifest = testManifest()
	manifest.ConfigSHA256 = repeat("ab", 32)
	if _, err := Resolve([]Copy{{Manifest: manifest, SHA256: repeat("11", 32)}}, Anchor{RepositoryID: "repo-a", Generation: 3, SHA256: repeat("22", 32)}); err == nil {
		t.Fatal("rolled-back topology was accepted")
	}
	if _, err := Resolve([]Copy{{Manifest: manifest, SHA256: repeat("11", 32)}, {Manifest: manifest, SHA256: repeat("22", 32)}}); err == nil {
		t.Fatal("same-generation conflict was accepted")
	}
}

func TestTopologyPublicationAndDiscoverySurviveIndividualMirrorLoss(t *testing.T) {
	manifest := testManifest()
	manifest.ConfigSHA256 = repeat("ab", 32)
	key := []byte("0123456789abcdef0123456789abcdef")
	encoded, digest, err := Seal(manifest, key)
	if err != nil {
		t.Fatal(err)
	}
	first, second := mem.New(), mem.New()
	mirrors := map[string]backend.Backend{"a": first, "b": second}
	if err := Publish(context.Background(), mirrors, manifest.Generation, encoded); err != nil {
		t.Fatal(err)
	}
	if err := Publish(context.Background(), mirrors, manifest.Generation, encoded); err != nil {
		t.Fatalf("same-byte publication was not idempotent: %v", err)
	}
	if err := Publish(context.Background(), mirrors, manifest.Generation, append(encoded, 'x')); err == nil {
		t.Fatal("conflicting immutable publication was accepted")
	}
	for id, seed := range mirrors {
		copies, failures := Discover(context.Background(), map[string]backend.Backend{id: seed}, key, "repo-a")
		if len(failures) != 0 || len(copies) != 1 || copies[0].SHA256 != digest {
			t.Fatalf("discovery from %s = %#v, %#v", id, copies, failures)
		}
	}
}

func TestTopologyRequiresCanonicalUniqueMirrorsAndContinuousKnownChain(t *testing.T) {
	manifest := testManifest()
	manifest.ConfigSHA256 = repeat("ab", 32)
	manifest.StagingBackends = []string{"a", "a"}
	if err := manifest.Validate(); err == nil {
		t.Fatal("duplicate staging mirrors were accepted")
	}
	manifest = testManifest()
	manifest.ConfigSHA256 = repeat("ab", 32)
	manifest.Backends[0], manifest.Backends[1] = manifest.Backends[1], manifest.Backends[0]
	if err := manifest.Validate(); err == nil {
		t.Fatal("non-canonical backend order was accepted")
	}
	previous := testManifest()
	previous.ConfigSHA256 = repeat("ab", 32)
	previous.Generation = 1
	current := testManifest()
	current.ConfigSHA256 = repeat("cd", 32)
	current.PreviousSHA256 = repeat("33", 32)
	if _, err := Resolve([]Copy{{Manifest: current, SHA256: repeat("22", 32)}, {Manifest: previous, SHA256: repeat("11", 32)}}); err == nil {
		t.Fatal("known discontinuous topology chain was accepted")
	}
}

func repeat(value string, count int) string {
	result := ""
	for range count {
		result += value
	}
	return result
}
