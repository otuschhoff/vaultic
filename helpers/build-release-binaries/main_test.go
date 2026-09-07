package main

import (
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
)

func platforms(targets map[string][]string) []string {
	result := make([]string, 0)
	for goos, architectures := range targets {
		for _, architecture := range architectures {
			result = append(result, goos+"/"+architecture)
		}
	}
	sort.Strings(result)
	return result
}

func TestDefaultBuildTargetsMatchReleasePolicy(t *testing.T) {
	want := []string{
		"darwin/arm64",
		"linux/amd64",
		"linux/arm64",
		"windows/amd64",
	}
	if got := platforms(defaultBuildTargets); !reflect.DeepEqual(got, want) {
		t.Fatalf("default build targets = %v, want %v", got, want)
	}
}

func TestPlatformSubsetsPartitionDefaultTargets(t *testing.T) {
	seen := make(map[string]bool)
	for _, subset := range []string{"0/3", "1/3", "2/3"} {
		targets, err := selectSubset(subset, defaultBuildTargets)
		if err != nil {
			t.Fatalf("selectSubset(%q): %v", subset, err)
		}
		for _, platform := range platforms(targets) {
			if seen[platform] {
				t.Fatalf("platform %q appears in more than one subset", platform)
			}
			seen[platform] = true
		}
	}

	want := platforms(defaultBuildTargets)
	got := make([]string, 0, len(seen))
	for platform := range seen {
		got = append(got, platform)
	}
	sort.Strings(got)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("partitioned platforms = %v, want %v", got, want)
	}
}

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
