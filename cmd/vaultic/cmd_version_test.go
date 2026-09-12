package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/otuschhoff/vaultic/internal/global"
)

func TestRootVersionFlagDoesNotRunCommandSetup(t *testing.T) {
	var output bytes.Buffer
	root := newRootCommand(&global.Options{})
	root.SetOut(&output)
	root.SetArgs([]string{"--version"})

	if err := root.Execute(); err != nil {
		t.Fatal(err)
	}
	report := output.String()
	if !strings.HasPrefix(report, "vaultic "+global.Version+" compiled with ") {
		t.Fatalf("unexpected version report: %q", report)
	}
	if !strings.Contains(report, "important dependencies:\n") {
		t.Fatalf("version report omits dependencies: %q", report)
	}
}

func TestVersionInfoIncludesSecurityDependency(t *testing.T) {
	info := collectVersionInfo()
	for _, dependency := range info.Dependencies {
		if dependency.Module == "golang.org/x/crypto" && dependency.Version != "" {
			return
		}
	}
	t.Fatal("version info omits golang.org/x/crypto")
}

func TestVersionInfoJSONIncludesDependencies(t *testing.T) {
	encoded, err := json.Marshal(collectVersionInfo())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(encoded, []byte(`"dependencies":[`)) {
		t.Fatalf("version JSON omits dependencies: %s", encoded)
	}
}
