// Package telemetry publishes backup summaries to external metrics systems.
package telemetry

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/otuschhoff/vaultic/internal/archiver"
)

// Config selects optional telemetry publishers.
type Config struct {
	PrometheusURL  string
	PrometheusUser string
	PrometheusPass string

	InfluxURL    string
	InfluxToken  string
	InfluxOrg    string
	InfluxBucket string
}

// Backup contains the stable backup result fields emitted by all publishers.
type Backup struct {
	Repository string
	SnapshotID string
	Label      string
	Summary    *archiver.Summary
}

// Publish sends a successful backup summary to every configured publisher.
func Publish(ctx context.Context, cfg Config, backup Backup) error {
	if backup.Summary == nil {
		return fmt.Errorf("backup summary is missing")
	}
	if cfg.PrometheusURL != "" {
		if err := publishPrometheus(ctx, cfg, backup); err != nil {
			return err
		}
	}
	if cfg.InfluxURL != "" {
		if cfg.InfluxToken == "" || cfg.InfluxOrg == "" || cfg.InfluxBucket == "" {
			return fmt.Errorf("--influxdb-url requires --influxdb-token, --influxdb-org, and --influxdb-bucket")
		}
		if err := publishInflux(ctx, cfg, backup); err != nil {
			return err
		}
	}
	return nil
}

func publishPrometheus(ctx context.Context, cfg Config, backup Backup) error {
	endpoint := strings.TrimRight(cfg.PrometheusURL, "/") + "/metrics/job/vaultic"
	body := prometheusText(backup)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint, strings.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "text/plain; version=0.0.4")
	if cfg.PrometheusUser != "" || cfg.PrometheusPass != "" {
		req.SetBasicAuth(cfg.PrometheusUser, cfg.PrometheusPass)
	}
	return send(req, "Prometheus Pushgateway")
}

func publishInflux(ctx context.Context, cfg Config, backup Backup) error {
	endpoint, err := url.Parse(cfg.InfluxURL)
	if err != nil {
		return fmt.Errorf("invalid InfluxDB URL: %w", err)
	}
	endpoint.Path = strings.TrimRight(endpoint.Path, "/")
	if !strings.HasSuffix(endpoint.Path, "/api/v2/write") {
		endpoint.Path += "/api/v2/write"
	}
	query := endpoint.Query()
	query.Set("org", cfg.InfluxOrg)
	query.Set("bucket", cfg.InfluxBucket)
	query.Set("precision", "ns")
	endpoint.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), strings.NewReader(influxLine(backup)))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Token "+cfg.InfluxToken)
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	return send(req, "InfluxDB")
}

func send(req *http.Request, target string) error {
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("publish to %s: %w", target, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("publish to %s: server returned %s", target, resp.Status)
	}
	return nil
}

func prometheusText(backup Backup) string {
	s := backup.Summary
	duration := s.BackupEnd.Sub(s.BackupStart).Seconds()
	return fmt.Sprintf("vaultic_backup_success 1\nvaultic_backup_duration_seconds %s\nvaultic_backup_files_processed %d\nvaultic_backup_bytes_processed %d\nvaultic_backup_data_added_bytes %d\nvaultic_backup_snapshot_info{snapshot=%q,label=%q,repository=%q} 1\n",
		strconv.FormatFloat(duration, 'f', -1, 64), s.Files.New+s.Files.Changed+s.Files.Unchanged, s.ProcessedBytes, s.DataSizeInRepo, backup.SnapshotID, backup.Label, backup.Repository)
}

func influxLine(backup Backup) string {
	s := backup.Summary
	tags := "repository=" + escapeTag(backup.Repository) + ",snapshot=" + escapeTag(backup.SnapshotID) + ",label=" + escapeTag(backup.Label)
	fields := fmt.Sprintf("success=1i,files_processed=%di,bytes_processed=%di,data_added=%di,duration_seconds=%s",
		s.Files.New+s.Files.Changed+s.Files.Unchanged, s.ProcessedBytes, s.DataSizeInRepo, strconv.FormatFloat(s.BackupEnd.Sub(s.BackupStart).Seconds(), 'f', -1, 64))
	return "vaultic_backup," + tags + " " + fields + " " + strconv.FormatInt(s.BackupEnd.UnixNano(), 10) + "\n"
}

func escapeTag(value string) string {
	return strings.NewReplacer(",", "\\,", " ", "\\ ", "=", "\\=").Replace(value)
}

// Timestamp is exposed to keep line protocol timestamp handling testable.
func Timestamp(t time.Time) int64 { return t.UnixNano() }
