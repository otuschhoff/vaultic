package indexcmd

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/index/daemon"
)

type stubWriterStatusProvider struct {
	status daemon.WriterStatus
	calls  int
}

func (provider *stubWriterStatusProvider) WriterStatus(context.Context) (daemon.WriterStatus, error) {
	provider.calls++
	return provider.status, nil
}

func TestStagingOptionsFinalize(t *testing.T) {
	tests := []struct {
		name    string
		options interface{ Finalize() error }
		wantErr string
	}{
		{name: "extend requires positive duration", options: stagingExtendOptions{}, wantErr: "--by must be positive"},
		{name: "extend valid", options: stagingExtendOptions{Extension: time.Second}},
		{name: "reject requires reason", options: stagingRejectOptions{}, wantErr: "rejection requires --reason"},
		{name: "reject valid", options: stagingRejectOptions{Reason: "corrupt"}},
		{
			name:    "abandon requires acknowledgement",
			options: stagingAbandonOptions{Reason: "lost", SafetyDelay: time.Hour},
			wantErr: "abandonment requires",
		},
		{
			name:    "abandon requires positive delay",
			options: stagingAbandonOptions{Reason: "lost", Acknowledge: true},
			wantErr: "--safety-delay must be positive",
		},
		{
			name:    "abandon valid",
			options: stagingAbandonOptions{Reason: "lost", Acknowledge: true, SafetyDelay: time.Hour},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.options.Finalize()
			if test.wantErr == "" && err != nil {
				t.Fatalf("Finalize() error = %v", err)
			}
			if test.wantErr != "" && (err == nil || !strings.Contains(err.Error(), test.wantErr)) {
				t.Fatalf("Finalize() error = %v, want containing %q", err, test.wantErr)
			}
		})
	}
}

func TestWriterPromoteOptionsFinalize(t *testing.T) {
	if err := (writerPromoteOptions{ForceTakeover: true, Timeout: time.Minute}).Finalize(); err == nil {
		t.Fatal("Finalize() accepted force takeover without an expected epoch")
	}
	if err := (writerPromoteOptions{ForceTakeover: true, ExpectedActiveEpoch: "1"}).Finalize(); err == nil {
		t.Fatal("Finalize() accepted a non-positive timeout")
	}
	if err := (writerPromoteOptions{ForceTakeover: true, ExpectedActiveEpoch: "invalid", Timeout: time.Minute}).Finalize(); err == nil {
		t.Fatal("Finalize() accepted an invalid expected epoch")
	}
	if err := (writerPromoteOptions{ForceTakeover: true, ExpectedActiveEpoch: "1", Timeout: time.Minute}).Finalize(); err != nil {
		t.Fatalf("Finalize() error = %v", err)
	}
	if err := (writerPromoteOptions{ForceTakeover: true, ExpectedActiveEpoch: "auto", Timeout: time.Minute}).Finalize(); err != nil {
		t.Fatalf("Finalize() error = %v", err)
	}
}

func TestParseExpectedActiveEpoch(t *testing.T) {
	tests := []struct {
		value     string
		wantEpoch uint64
		wantAuto  bool
		wantErr   bool
	}{
		{value: ""},
		{value: "42", wantEpoch: 42},
		{value: "auto", wantAuto: true},
		{value: "0", wantErr: true},
		{value: "latest", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.value, func(t *testing.T) {
			epoch, automatic, err := parseExpectedActiveEpoch(test.value)
			if (err != nil) != test.wantErr {
				t.Fatalf("parseExpectedActiveEpoch(%q) error = %v, wantErr %t", test.value, err, test.wantErr)
			}
			if epoch != test.wantEpoch || automatic != test.wantAuto {
				t.Fatalf("parseExpectedActiveEpoch(%q) = (%d, %t), want (%d, %t)", test.value, epoch, automatic, test.wantEpoch, test.wantAuto)
			}
		})
	}
}

func TestResolveExpectedActiveEpoch(t *testing.T) {
	provider := &stubWriterStatusProvider{status: daemon.WriterStatus{ObservedEpoch: 42}}
	automatic := writerPromoteOptions{ForceTakeover: true, ExpectedActiveEpoch: "auto"}
	epoch, err := automatic.resolveExpectedActiveEpoch(context.Background(), provider)
	if err != nil {
		t.Fatalf("resolveExpectedActiveEpoch() error = %v", err)
	}
	if epoch != 42 || provider.calls != 1 {
		t.Fatalf("resolveExpectedActiveEpoch() = %d with %d status calls, want 42 with 1", epoch, provider.calls)
	}

	explicit := writerPromoteOptions{ForceTakeover: true, ExpectedActiveEpoch: "7"}
	epoch, err = explicit.resolveExpectedActiveEpoch(context.Background(), provider)
	if err != nil {
		t.Fatalf("resolveExpectedActiveEpoch() error = %v", err)
	}
	if epoch != 7 || provider.calls != 1 {
		t.Fatalf("resolveExpectedActiveEpoch() = %d with %d status calls, want 7 with 1", epoch, provider.calls)
	}
}

func TestWriterPromoteCommandDefaults(t *testing.T) {
	command := newIndexWriterPromoteCommand(&global.Options{}, &indexWriterOptions{})
	expectedEpoch, err := command.Flags().GetString("expected-active-epoch")
	if err != nil {
		t.Fatalf("GetString(expected-active-epoch) error = %v", err)
	}
	if expectedEpoch != "auto" {
		t.Fatalf("expected-active-epoch default = %q, want auto", expectedEpoch)
	}
	timeout, err := command.Flags().GetDuration("timeout")
	if err != nil {
		t.Fatalf("GetDuration(timeout) error = %v", err)
	}
	if timeout != time.Hour {
		t.Fatalf("timeout default = %s, want 1h", timeout)
	}
}

func TestHealDecisionOptionsFinalize(t *testing.T) {
	if err := (indexHealDecisionOptions{}).finalize("activation"); err == nil {
		t.Fatal("activation accepted without approval")
	}
	if err := (indexHealDecisionOptions{Acknowledge: true}).finalize("rollback"); err != nil {
		t.Fatalf("rollback Finalize() error = %v", err)
	}
	if err := (indexHealDecisionOptions{Acknowledge: true}).finalize("retirement"); err == nil {
		t.Fatal("retirement accepted without generation")
	}
	if err := (indexHealDecisionOptions{Acknowledge: true, Generation: 1}).finalize("retirement"); err != nil {
		t.Fatalf("retirement Finalize() error = %v", err)
	}
}

func TestUnlockContributeOptionsFinalize(t *testing.T) {
	parent := indexUnlockOptions{Capsule: "capsule.json"}
	if err := (unlockContributeOptions{}).finalize(parent); err == nil {
		t.Fatal("contribution accepted without session file")
	}
	prepare := unlockContributeOptions{sessionFile: "session.json", prepare: true}
	if err := prepare.finalize(parent); err != nil {
		t.Fatalf("prepare Finalize() error = %v", err)
	}
	contribute := unlockContributeOptions{
		sessionFile: "session.json", memberID: "member", confirmedFingerprint: "fingerprint",
		credentials: unlockCredentialOptions{passphraseFile: "passphrase"},
	}
	if err := contribute.finalize(parent); err != nil {
		t.Fatalf("contribute Finalize() error = %v", err)
	}
	contribute.credentials.keyFile = "key"
	if err := contribute.finalize(parent); err == nil {
		t.Fatal("contribution accepted multiple credential routes")
	}
}

func TestQuorumBypassOptionsFinalize(t *testing.T) {
	statements := quorumBypassStatements{
		NoPlaintextKeyExports: true, NoExternalStandaloneEscrow: true, GenerationAnchorsCurrent: true,
		BrokerCredentialsProtected: true, NoWarmRestartMaterial: true, OfflineSharesSeparate: true,
	}
	if err := (quorumBypassOptions{Statements: statements}).finalize(); err == nil {
		t.Fatal("attestation accepted a zero validity duration")
	}
	if err := (quorumBypassOptions{Statements: statements, ValidFor: time.Hour}).finalize(); err != nil {
		t.Fatalf("attestation Finalize() error = %v", err)
	}
	statements.NoWarmRestartMaterial = false
	if err := (quorumBypassOptions{Statements: statements, ValidFor: time.Hour}).finalize(); err == nil {
		t.Fatal("attestation accepted an incomplete inventory")
	}
}

func TestKeyOptionsFinalize(t *testing.T) {
	prepare := quorumPrepareOptions{
		CapsuleDirectory: "capsules", GroupID: "operators", BrokerPublicKeyFile: "broker.pub",
		StateFile: "state.json", TopologyFile: "topology.json", Generation: 1, Threshold: 2,
	}
	if err := prepare.finalize(1); err == nil {
		t.Fatal("quorum preparation accepted an unsatisfiable threshold")
	}
	if err := prepare.finalize(2); err != nil {
		t.Fatalf("quorum preparation Finalize() error = %v", err)
	}
	if err := (keyStatusOptions{Capsule: "capsule.json"}).finalize(""); err == nil {
		t.Fatal("key status accepted a capsule without a broker socket")
	}
	if err := (keyStatusOptions{Capsule: "capsule.json"}).finalize("broker.sock"); err != nil {
		t.Fatalf("key status Finalize() error = %v", err)
	}
}

func TestAnalyticsOptionsFinalize(t *testing.T) {
	options := indexAnalyticsOptions{SizeMin: 10, SizeMax: 10, HasSizeMin: true, HasSizeMax: true}
	if err := validateAnalyticsJobOptions(options); err == nil {
		t.Fatal("analytics accepted an empty exclusive size range")
	}
	options = indexAnalyticsOptions{Async: true, QueryID: strings.Repeat("0", 64)}
	if err := validateAnalyticsJobOptions(options); err == nil {
		t.Fatal("analytics accepted --async with --query-id")
	}

	growth := indexGrowthOptions{Since: "2026-01-02T00:00:00Z", Until: "2026-01-01T00:00:00Z"}
	if err := growth.finalize(); err == nil {
		t.Fatal("growth accepted an inverted time range")
	}
	growth.Until = "2026-01-03T00:00:00Z"
	if err := growth.finalize(); err != nil || growth.FinalSince == nil || growth.FinalUntil == nil {
		t.Fatalf("growth Finalize() = (%v, %v, %v)", growth.FinalSince, growth.FinalUntil, err)
	}
}

func TestGDPRErrorOptionsFinalize(t *testing.T) {
	if err := (gdprUIDOptions{Flag: "uid"}).finalize(); err == nil {
		t.Fatal("GDPR UID options accepted a missing UID")
	}
	if err := (gdprUIDOptions{UID: 42, UIDChanged: true, Flag: "uid"}).finalize(); err != nil {
		t.Fatalf("GDPR UID Finalize() error = %v", err)
	}
	if err := (gdprExecuteOptions{UID: 42, UIDChanged: true}).finalize(); err == nil {
		t.Fatal("GDPR execution accepted missing confirmation")
	}
	certificate := gdprCertificateOptions{UID: 42, UIDChanged: true, ExecutedAt: 1, RunIDValue: "run", PublicKeyFile: "key"}
	if err := certificate.finalize(); err != nil {
		t.Fatalf("GDPR certificate Finalize() error = %v", err)
	}
}
