package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/index/maintenance"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

// countingBackend records every List call so a test can assert that --no-list
// really performed zero backend requests rather than merely reporting nothing.
type countingBackend struct {
	location string
	files    map[backend.FileType][]backend.FileInfo
	listCall int
}

func (target *countingBackend) Properties() backend.Properties {
	return backend.Properties{Connections: 2}
}

func (target *countingBackend) Location() string { return target.location }

func (target *countingBackend) List(_ context.Context, fileType backend.FileType, fn func(backend.FileInfo) error) error {
	target.listCall++
	for _, info := range target.files[fileType] {
		if err := fn(info); err != nil {
			return err
		}
	}
	return nil
}

// TestBackendsNoListPerformsZeroBackendRequests is the archival guarantee: on a
// backend where listing costs money or hours, --no-list must not touch it.
func TestBackendsNoListPerformsZeroBackendRequests(t *testing.T) {
	hot := &countingBackend{location: "hot:/x", files: map[backend.FileType][]backend.FileInfo{
		backend.PackFile: {{Name: "aa", Size: 10}},
	}}
	cold := &countingBackend{location: "cold:/y", files: map[backend.FileType][]backend.FileInfo{
		backend.PackFile: {{Name: "bb", Size: 20}},
	}}
	targets := []backendTarget{{id: "hot", role: "hot", ingest: true, readEnabled: true, lister: hot}, {id: "cold", role: "cold", ingest: true, readEnabled: true, lister: cold}}

	reports, err := collectBackendReports(context.Background(), targets, true)
	if err != nil {
		t.Fatal(err)
	}
	if hot.listCall != 0 || cold.listCall != 0 {
		t.Fatalf("--no-list issued backend requests: hot=%d cold=%d", hot.listCall, cold.listCall)
	}
	for _, report := range reports {
		if report.Listed {
			t.Fatalf("%s backend was marked as listed under --no-list", report.Role)
		}
		if len(report.FileTypes) != 0 {
			t.Fatalf("%s backend reported object counts it never obtained: %#v", report.Role, report.FileTypes)
		}
		if report.Location == "" {
			t.Fatalf("%s backend lost its location, which needs no listing", report.Role)
		}
	}

	// Without --no-list the same backends must actually be listed, so the
	// assertion above is about the flag and not about a broken harness.
	if _, err := collectBackendReports(context.Background(), targets, false); err != nil {
		t.Fatal(err)
	}
	if hot.listCall == 0 || cold.listCall == 0 {
		t.Fatalf("listing was skipped without --no-list: hot=%d cold=%d", hot.listCall, cold.listCall)
	}
}

func TestPlacementBackendReportsNoListIncludesIngestAndReadFlags(t *testing.T) {
	readOnly := false
	model := repository.PlacementModel{Backends: []repository.PlacementBackend{
		{PlacementBackend: vaultic.PlacementBackend{ID: "legacy", Role: "archival", Location: "s3:old", Ingest: &readOnly, ReadEnabled: boolPtr(true)}},
		{PlacementBackend: vaultic.PlacementBackend{ID: "active", Role: "archival", Location: "s3:new"}},
	}}
	reports := placementBackendReportsNoList(model)
	if len(reports) != 2 {
		t.Fatalf("reports = %#v", reports)
	}
	if reports[0].ID != "legacy" || reports[0].Ingest || !reports[0].ReadEnabled {
		t.Fatalf("legacy report = %#v", reports[0])
	}
	if reports[1].ID != "active" || !reports[1].Ingest || !reports[1].ReadEnabled {
		t.Fatalf("active report = %#v", reports[1])
	}
}

func boolPtr(value bool) *bool { return &value }

// TestBackendsCompareReportsMissingAndExtraSeparately: a pack the catalog
// claims but the backend lacks is data loss, while a pack the backend holds
// that the catalog does not know is only waste. They must never be conflated.
func TestBackendsCompareReportsMissingAndExtraSeparately(t *testing.T) {
	known := map[string]struct{}{"shared": {}, "deliberately-missing": {}}
	present := map[string]struct{}{"shared": {}, "deliberately-extra": {}}

	missing, unknown := diffCatalogAgainstListing(known, present)
	if len(missing) != 1 || missing[0] != "deliberately-missing" {
		t.Fatalf("missing objects = %#v, want [deliberately-missing]", missing)
	}
	if len(unknown) != 1 || unknown[0] != "deliberately-extra" {
		t.Fatalf("unknown objects = %#v, want [deliberately-extra]", unknown)
	}
}

// TestBackendsCompareIsStablyOrdered keeps the JSON contract deterministic
// despite the map iteration underneath.
func TestBackendsCompareIsStablyOrdered(t *testing.T) {
	known := map[string]struct{}{"c": {}, "b": {}, "a": {}}
	present := map[string]struct{}{"z": {}, "y": {}, "x": {}}
	for range 20 {
		missing, unknown := diffCatalogAgainstListing(known, present)
		if missing[0] != "a" || missing[1] != "b" || missing[2] != "c" {
			t.Fatalf("missing objects are not sorted: %#v", missing)
		}
		if unknown[0] != "x" || unknown[1] != "y" || unknown[2] != "z" {
			t.Fatalf("unknown objects are not sorted: %#v", unknown)
		}
	}
}

// TestCompareDoesNotReportRoutineDeletionAsDataLoss: a pack awaiting deletion,
// already deleted, or recorded as orphaned is absent from the backend by
// design. Reporting it as missing would turn every prune into an alarm.
func TestCompareDoesNotReportRoutineDeletionAsDataLoss(t *testing.T) {
	for _, state := range []string{"delete-pending", "deleted", "orphaned", "unknown", ""} {
		if packShouldExistOnBackend(state) {
			t.Errorf("state %q was expected to have a backend object", state)
		}
	}
	for _, state := range []string{"imported", "published", "export-pending"} {
		if !packShouldExistOnBackend(state) {
			t.Errorf("state %q should have a backend object but was treated as absent by design", state)
		}
	}
}

// TestBackendsRejectsCompareWithNoList: --compare needs a listing by
// definition, so combining the flags must fail loudly rather than silently
// dropping one of them.
func TestBackendsRejectsCompareWithNoList(t *testing.T) {
	options := indexBackendsOptions{Compare: true, NoList: true}
	_, err := runIndexBackends(context.Background(), options, global.Options{}, nil)
	if err == nil {
		t.Fatal("--compare with --no-list was accepted")
	}
}

// TestBackendsGoldenOutput pins the `index backends --json` contract.
func TestBackendsGoldenOutput(t *testing.T) {
	result := BackendsResult{
		SchemaVersion: maintenance.IntrospectSchemaVersion,
		HotCold:       true,
		Backends: []BackendReport{
			{ID: "hot", Role: "hot", Location: "hot:/x", Ingest: true, ReadEnabled: true, Connections: 2, Listed: true, FileTypes: []BackendFileTypeCount{
				{FileType: "pack", Objects: 1, Bytes: 10},
			}},
			{ID: "cold", Role: "cold", Location: "cold:/y", Ingest: false, ReadEnabled: true, Connections: 2, Listed: false},
		},
		Compared: true, CatalogPacks: 2, BackendPacks: 2,
		MissingOnBackend: []string{"deliberately-missing"}, UnknownToCatalog: []string{"deliberately-extra"},
		MissingOnBackendNum: 1, UnknownToCatalogNum: 1,
	}
	encoded, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	encoded = append(encoded, '\n')
	path := filepath.Join("testdata", "index_backends.json")
	if os.Getenv("UPDATE_GOLDEN") != "" {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, encoded, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	expected, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s (set UPDATE_GOLDEN=1 to create it): %v", path, err)
	}
	if string(expected) != string(encoded) {
		t.Fatalf("golden %s mismatch:\nwant:\n%s\ngot:\n%s", path, expected, encoded)
	}
}

func TestPlacementGoldenOutput(t *testing.T) {
	result := maintenance.PlacementSchedulerResult{
		SchemaVersion:             maintenance.IntrospectSchemaVersion,
		PacksScanned:              3,
		Unsatisfied:               1,
		Overdue:                   1,
		PendingPromotion:          1,
		OldestUnsatisfiedDeadline: 1_700_000_000_000_000_000,
		RequestsWritten:           2,
		Worker: &maintenance.PlacementWorkerResult{
			RequestsScanned: 2, Attempted: 1, Placed: 1, Deferred: 1, BytesMoved: 4096,
		},
		Statuses: []maintenance.PlacementStatus{{
			PackID: "012345", PackType: "data", Class: "recent-data",
			TargetBackends: []string{"local", "warm"}, LiveBackends: []string{"local"},
			MissingBackends: []string{"warm"}, Durable: false, Overdue: true,
			Deadline: 1_700_000_000_000_000_000,
		}},
	}
	encoded, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	encoded = append(encoded, '\n')
	path := filepath.Join("testdata", "index_placement.json")
	if os.Getenv("UPDATE_GOLDEN") != "" {
		if err := os.WriteFile(path, encoded, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	expected, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s (set UPDATE_GOLDEN=1 to create it): %v", path, err)
	}
	if string(expected) != string(encoded) {
		t.Fatalf("golden %s mismatch:\nwant:\n%s\ngot:\n%s", path, expected, encoded)
	}
}

// TestLegacyRepositoryErrorIsIdentifiable lets callers and tests distinguish
// "this repository cannot answer" from an incidental failure.
func TestLegacyRepositoryErrorIsIdentifiable(t *testing.T) {
	wrapped := errors.New("outer: " + maintenance.ErrLegacyRepository.Error())
	if errors.Is(wrapped, maintenance.ErrLegacyRepository) {
		t.Fatal("a string-formatted error should not satisfy errors.Is")
	}
	if !errors.Is(maintenance.ErrLegacyRepository, maintenance.ErrLegacyRepository) {
		t.Fatal("the sentinel does not match itself")
	}
}
