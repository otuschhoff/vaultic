package observability

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Severity uint8

const (
	Emergency Severity = iota
	Alert
	Critical
	Error
	Warning
	Notice
	Info
	Debug
)

var severityNames = map[string]Severity{
	"emergency": Emergency,
	"alert":     Alert,
	"critical":  Critical,
	"error":     Error,
	"warning":   Warning,
	"notice":    Notice,
	"info":      Info,
	"debug":     Debug,
}

type Category string

const (
	CategoryAuth      Category = "auth"
	CategoryIntegrity Category = "integrity"
	CategoryGDPR      Category = "gdpr"
	CategoryRestore   Category = "restore"
	CategoryLifecycle Category = "lifecycle"
)

var validCategories = map[Category]bool{CategoryAuth: true, CategoryIntegrity: true, CategoryGDPR: true, CategoryRestore: true, CategoryLifecycle: true}

type Event struct {
	Time      time.Time      `json:"time"`
	Severity  Severity       `json:"severity"`
	Category  Category       `json:"category"`
	Component string         `json:"component"`
	Message   string         `json:"message"`
	Fields    map[string]any `json:"fields,omitempty"`
}

type SyslogTarget struct {
	Network     string
	Address     string
	Format      string
	Facility    int
	MaxSeverity Severity
	Categories  map[Category]bool
	Timeout     time.Duration
	TLSConfig   *tls.Config
}

func ParseSyslogTarget(spec string) (SyslogTarget, error) {
	parsed, err := url.Parse(spec)
	if err != nil {
		return SyslogTarget{}, fmt.Errorf("parse syslog target: %w", err)
	}
	target := SyslogTarget{Network: parsed.Scheme, Address: parsed.Host, Format: "rfc5424", Facility: 1, MaxSeverity: Debug, Timeout: 10 * time.Second}
	if parsed.Scheme == "unix" || parsed.Scheme == "unixgram" {
		target.Address = parsed.Path
	}
	switch target.Network {
	case "udp", "tcp", "tls", "unix", "unixgram":
	default:
		return SyslogTarget{}, fmt.Errorf("unsupported syslog transport %q", target.Network)
	}
	if target.Address == "" {
		return SyslogTarget{}, fmt.Errorf("syslog target address is required")
	}
	query := parsed.Query()
	if value := query.Get("format"); value != "" {
		target.Format = strings.ToLower(value)
	}
	if target.Format != "rfc5424" && target.Format != "rfc3164" {
		return SyslogTarget{}, fmt.Errorf("unsupported syslog format %q", target.Format)
	}
	if value := query.Get("facility"); value != "" {
		target.Facility, err = strconv.Atoi(value)
		if err != nil || target.Facility < 0 || target.Facility > 23 {
			return SyslogTarget{}, fmt.Errorf("invalid syslog facility %q", value)
		}
	}
	if value := query.Get("min-severity"); value != "" {
		var ok bool
		target.MaxSeverity, ok = severityNames[strings.ToLower(value)]
		if !ok {
			return SyslogTarget{}, fmt.Errorf("invalid syslog severity %q", value)
		}
	}
	if value := query.Get("categories"); value != "" {
		target.Categories = make(map[Category]bool)
		for item := range strings.SplitSeq(value, ",") {
			category := Category(strings.TrimSpace(item))
			if !validCategories[category] {
				return SyslogTarget{}, fmt.Errorf("invalid syslog category %q", category)
			}
			target.Categories[category] = true
		}
	}
	if value := query.Get("timeout"); value != "" {
		target.Timeout, err = time.ParseDuration(value)
		if err != nil || target.Timeout <= 0 {
			return SyslogTarget{}, fmt.Errorf("invalid syslog timeout %q", value)
		}
	}
	if target.Network == "tls" {
		target.TLSConfig, err = parseSyslogTLSConfig(parsed)
		if err != nil {
			return SyslogTarget{}, err
		}
	}
	return target, nil
}

func parseSyslogTLSConfig(parsed *url.URL) (*tls.Config, error) {
	query := parsed.Query()
	serverName := query.Get("server-name")
	if serverName == "" {
		serverName = parsed.Hostname()
	}
	config := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: serverName}
	if query.Get("insecure") == "true" {
		config.InsecureSkipVerify = true
	}
	if filename := query.Get("ca"); filename != "" {
		pem, err := os.ReadFile(filename)
		if err != nil {
			return nil, fmt.Errorf("read syslog CA: %w", err)
		}
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("syslog CA file contains no certificates")
		}
		config.RootCAs = pool
	}
	certFile, keyFile := query.Get("cert"), query.Get("key")
	if (certFile == "") != (keyFile == "") {
		return nil, fmt.Errorf("syslog client certificate and key must be specified together")
	}
	if certFile != "" {
		certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("load syslog client certificate: %w", err)
		}
		config.Certificates = []tls.Certificate{certificate}
	}
	return config, nil
}

type SyslogExporter struct {
	targets  []SyslogTarget
	hostname string
	appName  string
}

func NewSyslogExporter(targets []SyslogTarget, hostname, appName string) *SyslogExporter {
	return &SyslogExporter{targets: append([]SyslogTarget(nil), targets...), hostname: sanitizeSyslogToken(hostname), appName: sanitizeSyslogToken(appName)}
}

func (exporter *SyslogExporter) Emit(ctx context.Context, event Event) error {
	if event.Time.IsZero() {
		event.Time = time.Now().UTC()
	}
	if !validCategories[event.Category] || event.Severity > Debug || strings.TrimSpace(event.Message) == "" {
		return fmt.Errorf("invalid structured event")
	}
	var failures []error
	for _, target := range exporter.targets {
		if event.Severity > target.MaxSeverity || (len(target.Categories) > 0 && !target.Categories[event.Category]) {
			continue
		}
		message, err := exporter.format(target, event)
		if err == nil {
			err = sendSyslog(ctx, target, message)
		}
		if err != nil {
			failures = append(failures, fmt.Errorf("%s://%s: %w", target.Network, target.Address, err))
		}
	}
	return errors.Join(failures...)
}

func (exporter *SyslogExporter) format(target SyslogTarget, event Event) ([]byte, error) {
	payload, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("marshal structured event: %w", err)
	}
	priority := target.Facility*8 + int(event.Severity)
	if target.Format == "rfc3164" {
		return fmt.Appendf(nil, "<%d>%s %s %s: %s", priority, event.Time.Format("Jan _2 15:04:05"), exporter.hostname, exporter.appName, payload), nil
	}
	return fmt.Appendf(
		nil,
		"<%d>1 %s %s %s - %s - %s",
		priority,
		event.Time.UTC().Format(time.RFC3339Nano),
		exporter.hostname,
		exporter.appName,
		sanitizeSyslogToken(string(event.Category)),
		payload,
	), nil
}

func sendSyslog(ctx context.Context, target SyslogTarget, message []byte) error {
	dialer := &net.Dialer{Timeout: target.Timeout}
	var connection net.Conn
	var err error
	if target.Network == "tls" {
		connection, err = tls.DialWithDialer(dialer, "tcp", target.Address, target.TLSConfig.Clone())
	} else {
		connection, err = dialer.DialContext(ctx, target.Network, target.Address)
	}
	if err != nil {
		return err
	}
	defer connection.Close()
	if deadline, ok := ctx.Deadline(); ok {
		if err := connection.SetDeadline(deadline); err != nil {
			return err
		}
	} else {
		if err := connection.SetDeadline(time.Now().Add(target.Timeout)); err != nil {
			return err
		}
	}
	if target.Network == "tcp" || target.Network == "tls" || target.Network == "unix" {
		message = fmt.Appendf(nil, "%d %s", len(message), message)
	}
	_, err = connection.Write(message)
	return err
}

func sanitizeSyslogToken(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "-"
	}
	return strings.Map(func(character rune) rune {
		if character <= ' ' || character == 127 || character == ']' || character == '"' || character == '=' {
			return '_'
		}
		return character
	}, value)
}

var defaultSyslog struct {
	sync.RWMutex
	exporter *SyslogExporter
}

func SetDefaultSyslog(exporter *SyslogExporter) {
	defaultSyslog.Lock()
	defer defaultSyslog.Unlock()
	defaultSyslog.exporter = exporter
}

func Emit(ctx context.Context, event Event) error {
	defaultSyslog.RLock()
	exporter := defaultSyslog.exporter
	defaultSyslog.RUnlock()
	if exporter == nil {
		return nil
	}
	err := exporter.Emit(ctx, event)
	if err != nil {
		log.Printf("observability: emit %s/%s event: %v", event.Category, event.Component, err)
	}
	return err
}

// EmitBestEffort emits advisory telemetry without changing the caller's outcome.
func EmitBestEffort(ctx context.Context, event Event) {
	// Emit logs delivery failures, so callers do not need a second reporting path.
	_ = Emit(ctx, event)
}
