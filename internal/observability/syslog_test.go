package observability

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"
)

func TestSyslogUDPFilteringAndRouting(t *testing.T) {
	listener, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	target, err := ParseSyslogTarget(
		"udp://" + listener.LocalAddr().String() + "?categories=auth,integrity&min-severity=warning",
	)
	if err != nil {
		t.Fatal(err)
	}
	exporter := NewSyslogExporter([]SyslogTarget{target}, "host", "vaultic")
	if err := exporter.Emit(context.Background(), Event{Severity: Info, Category: CategoryAuth, Message: "filtered"}); err != nil {
		t.Fatal(err)
	}
	if err := exporter.Emit(context.Background(), Event{Severity: Error, Category: CategoryGDPR, Message: "filtered"}); err != nil {
		t.Fatal(err)
	}
	if err := exporter.Emit(context.Background(),
		Event{Time: time.Unix(100,
			0),
			Severity:  Error,
			Category:  CategoryIntegrity,
			Component: "index",
			Message:   "checksum mismatch",
			Fields:    map[string]any{"pack": "abc"}}); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 4096)
	if err := listener.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	count, _, err := listener.ReadFrom(buffer)
	if err != nil {
		t.Fatal(err)
	}
	message := string(buffer[:count])
	if !strings.HasPrefix(message, "<11>1 ") || !strings.Contains(message, `"category":"integrity"`) ||
		!strings.Contains(message, `"pack":"abc"`) {
		t.Fatalf("unexpected RFC5424 message %q", message)
	}
	if err := listener.SetReadDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := listener.ReadFrom(buffer); err == nil {
		t.Fatal("filtered event was unexpectedly delivered")
	}
}

func TestSyslogTLSSendingUsesOctetCountFraming(t *testing.T) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
	}
	certificateDER, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := tls.Listen(
		"tcp",
		"127.0.0.1:0",
		&tls.Config{
			MinVersion:   tls.VersionTLS12,
			Certificates: []tls.Certificate{{Certificate: [][]byte{certificateDER}, PrivateKey: privateKey}},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	received := make(chan string, 1)
	errors := make(chan error, 1)
	go func() {
		connection, err := listener.Accept()
		if err != nil {
			errors <- err
			return
		}
		defer connection.Close()
		data, err := io.ReadAll(connection)
		if err != nil {
			errors <- err
			return
		}
		received <- string(data)
	}()
	target := SyslogTarget{
		Network:     "tls",
		Address:     listener.Addr().String(),
		Format:      "rfc5424",
		Facility:    1,
		MaxSeverity: Debug,
		Timeout:     time.Second,
		TLSConfig:   &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: true},
	}
	exporter := NewSyslogExporter([]SyslogTarget{target}, "host", "vaultic")
	if err := exporter.Emit(context.Background(),
		Event{Time: time.Unix(100,
			0),
			Severity:  Notice,
			Category:  CategoryGDPR,
			Component: "index",
			Message:   "forget committed"}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errors:
		t.Fatal(err)
	case message := <-received:
		separator := strings.IndexByte(message, ' ')
		if separator <= 0 || !strings.HasPrefix(message[separator+1:], "<13>1 ") ||
			!strings.Contains(message, `"category":"gdpr"`) {
			t.Fatalf("unexpected TLS syslog frame %q", message)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for TLS syslog message")
	}
}

func TestParseSyslogTargetValidation(t *testing.T) {
	for _, spec := range []string{"http://host", "udp://", "udp://host?format=x", "udp://host?categories=unknown", "tls://host?cert=a"} {
		if _, err := ParseSyslogTarget(spec); err == nil {
			t.Fatalf("expected %q to fail", spec)
		}
	}
}
