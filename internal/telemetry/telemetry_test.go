package telemetry

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/archiver"
)

func testBackup() Backup {
	return Backup{Repository: "repo one", SnapshotID: "abc", Label: "daily", Summary: &archiver.Summary{BackupStart: time.Unix(0, 0), BackupEnd: time.Unix(2, 0), ProcessedBytes: 42}}
}

func TestPublishInfluxV2(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v2/write" || r.URL.Query().Get("org") != "acme" || r.URL.Query().Get("bucket") != "backups" {
			t.Fatalf("unexpected request %s?%s", r.URL.Path, r.URL.RawQuery)
		}
		if r.Header.Get("Authorization") != "Token secret" {
			t.Fatalf("authorization = %q", r.Header.Get("Authorization"))
		}
		body, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(body), "vaultic_backup,repository=repo\\ one") {
			t.Fatalf("unexpected line protocol %q", body)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	if err := Publish(context.Background(), Config{InfluxURL: server.URL, InfluxToken: "secret", InfluxOrg: "acme", InfluxBucket: "backups"}, testBackup()); err != nil {
		t.Fatal(err)
	}
}

func TestPublishPrometheus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/metrics/job/vaultic" {
			t.Fatalf("path = %q", r.URL.Path)
		}
		body, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(body), "vaultic_backup_success 1") {
			t.Fatalf("unexpected metrics %q", body)
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()
	if err := Publish(context.Background(), Config{PrometheusURL: server.URL}, testBackup()); err != nil {
		t.Fatal(err)
	}
}
