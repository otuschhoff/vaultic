package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestStageLicense(t *testing.T) {
	sourceDir := t.TempDir()
	outputDir := t.TempDir()
	want := []byte("canonical license text\n")
	if err := os.WriteFile(filepath.Join(sourceDir, "LICENSE"), want, 0644); err != nil {
		t.Fatal(err)
	}

	if err := stageLicense(sourceDir, outputDir); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(outputDir, "LICENSE"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("staged license = %q, want %q", got, want)
	}
}

func TestStageLicenseRequiresSourceNotice(t *testing.T) {
	err := stageLicense(t.TempDir(), t.TempDir())
	if err == nil {
		t.Fatal("stageLicense succeeded without a source LICENSE")
	}
}
