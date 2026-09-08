package global

import (
	"testing"

	"github.com/otuschhoff/vaultic/internal/topology"
)

func TestApplyTopologyOverridesRestrictsChangesToLocalDataDir(t *testing.T) {
	document := topology.Document{
		PackBackends: []topology.PackBackend{
			{ID: "local", Provider: topology.ProviderLocal, Endpoint: map[string]any{"data_dir": "/old"}},
			{ID: "remote", Provider: topology.ProviderS3, Endpoint: map[string]any{"url": "https://example.invalid", "bucket": "bucket", "region": "region"}},
		},
	}
	if err := applyTopologyOverrides(&document, []string{"local.data_dir=/new"}); err != nil {
		t.Fatal(err)
	}
	if document.PackBackends[0].Endpoint["data_dir"] != "/new" {
		t.Fatalf("local override was not applied: %#v", document.PackBackends[0].Endpoint)
	}
	for _, invalid := range []string{"remote.data_dir=/new", "local.url=https://other.invalid", "local.data_dir="} {
		if err := applyTopologyOverrides(&document, []string{invalid}); err == nil {
			t.Fatalf("override %q was accepted", invalid)
		}
	}
}
