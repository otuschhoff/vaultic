package telemetry

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDecodeExperimentProfileRejectsOversizedDocument(t *testing.T) {
	_, err := DecodeExperimentProfile(make([]byte, MaxExperimentDocumentBytes+1))
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversized document error = %v", err)
	}
}

func TestExperimentProfileRejectsUnsupportedOperationRole(t *testing.T) {
	profile := validExperimentProfile()
	profile.Operation = "forget"
	profile.Role = "source"
	if err := profile.Validate(); err == nil || !strings.Contains(err.Error(), "operation/role") {
		t.Fatalf("operation/role error = %v", err)
	}
}

func TestExperimentProfileRejectsCallerHoldingServiceLock(t *testing.T) {
	profile := validExperimentProfile()
	profile.Holds = []string{"lock"}
	if err := profile.Validate(); err == nil || !strings.Contains(err.Error(), "cannot hold") {
		t.Fatalf("caller hold error = %v", err)
	}
}

func validExperimentProfile() ExperimentProfile {
	return ExperimentProfile{
		SchemaVersion:   ExperimentSchemaVersion,
		ProfileID:       "s3-commit-ack-8ms",
		Enabled:         true,
		TestOnly:        true,
		Scenario:        "s3",
		Backend:         "rpc",
		Mode:            DelayAcknowledgement,
		Operation:       "legacy_import",
		Role:            "rpc",
		Method:          "commit",
		AccessPattern:   "not_applicable",
		TargetID:        "isolated-repository",
		ResourceID:      "s3-link",
		Placement:       "after_completion",
		Latency:         LatencyAcknowledgementWindow,
		Interpretation:  "additive",
		Endpoint:        "caller",
		Acknowledgement: "applied",
		DelayUS:         8000,
		Concurrency:     4,
		DeadlineMS:      30000,
		MaxRetries:      3,
		RetryError:      "none",
		Seed:            34,
		Holds:           []string{"none"},
		Unknowns:        []ExperimentUnknown{{Field: "provider_jitter", Reason: "not measured on the isolated fixture"}},
	}
}

func TestSharedProfileFixtures(t *testing.T) {
	for _, name := range []string{"nfs-hdd.json", "rados-three-replica.json", "s3-8ms-rtt.json"} {
		encoded, err := os.ReadFile(filepath.Join("testdata", "phase34", name))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := DecodeExperimentProfile(encoded); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
}

func TestExperimentProfileStrictRoundTrip(t *testing.T) {
	want := validExperimentProfile()
	encoded, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeExperimentProfile(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"unknowns"`) || got.Unknowns[0] != want.Unknowns[0] {
		t.Fatalf("explicit unknown was not preserved: %#v", got.Unknowns)
	}
}

func TestExperimentProfileRejectsUnknownAndUnboundedParameters(t *testing.T) {
	encoded, err := json.Marshal(validExperimentProfile())
	if err != nil {
		t.Fatal(err)
	}
	encoded = append(encoded[:len(encoded)-1], []byte(`,"repository_path":"/secret"}`)...)
	if _, err := DecodeExperimentProfile(encoded); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("unknown profile field error = %v", err)
	}
	profile := validExperimentProfile()
	profile.Holds = []string{"object_key"}
	if err := profile.Validate(); err == nil || !strings.Contains(err.Error(), "unbounded") {
		t.Fatalf("unbounded profile dimension error = %v", err)
	}
}

func TestExperimentProfileDistinguishesS3RTTFromServiceLatency(t *testing.T) {
	profile := validExperimentProfile()
	profile.Mode = DelayService
	profile.Placement = "inside_service"
	profile.Latency = LatencyNetworkRTT
	if err := profile.Validate(); err == nil || !strings.Contains(err.Error(), "incompatible") {
		t.Fatalf("S3 RTT/service validation error = %v", err)
	}
	profile.Mode = DelayDisabled
	profile.Enabled = false
	profile.DelayUS = 0
	profile.RetryError = ""
	profile.Concurrency = 0
	profile.DeadlineMS = 0
	profile.MaxRetries = 0
	if err := profile.Validate(); err != nil {
		t.Fatalf("disabled S3 RTT assumption was rejected: %v", err)
	}
}

func TestExperimentProfileBoundsDisabledAndTailParameters(t *testing.T) {
	profile := validExperimentProfile()
	profile.Mode = DelayDisabled
	profile.Enabled = false
	if err := profile.Validate(); err == nil || !strings.Contains(err.Error(), "active injection") {
		t.Fatalf("disabled active profile error = %v", err)
	}
	profile = validExperimentProfile()
	profile.TailEvery = MaxExperimentFrequency + 1
	profile.TailDelayUS = 1
	if err := profile.Validate(); err == nil || !strings.Contains(err.Error(), "tail frequency") {
		t.Fatalf("tail bound error = %v", err)
	}
}

func TestExperimentProfileRejectsAmbiguousModesAndHolds(t *testing.T) {
	profile := validExperimentProfile()
	profile.Mode = DelayService
	if err := profile.Validate(); err == nil || !strings.Contains(err.Error(), "incompatible") {
		t.Fatalf("ambiguous service mode error = %v", err)
	}
	profile = validExperimentProfile()
	profile.Holds = []string{"none", "lock"}
	if err := profile.Validate(); err == nil || !strings.Contains(err.Error(), "exclusive") {
		t.Fatalf("contradictory holds error = %v", err)
	}
}

func TestExperimentProfileRejectsDisabledResourceHold(t *testing.T) {
	profile := validExperimentProfile()
	profile.Enabled = false
	profile.Mode = DelayDisabled
	profile.DelayUS = 0
	profile.Concurrency = 0
	profile.DeadlineMS = 0
	profile.MaxRetries = 0
	profile.RetryError = ""
	profile.Holds = []string{"connection"}
	if err := profile.Validate(); err == nil || !strings.Contains(err.Error(), "hold exactly none") {
		t.Fatalf("disabled hold error = %v", err)
	}
}

func TestExperimentProfileRequiresExplicitHolds(t *testing.T) {
	profile := validExperimentProfile()
	profile.Holds = nil
	if err := profile.Validate(); err == nil || !strings.Contains(err.Error(), "explicitly record") {
		t.Fatalf("missing holds error = %v", err)
	}
}

func TestExperimentProfileRequiresMatchingConfirmedDisposableTarget(t *testing.T) {
	profile := validExperimentProfile()
	for _, target := range []ExperimentTarget{
		{ID: profile.TargetID, Disposable: false, Confirmed: true},
		{ID: profile.TargetID, Disposable: true, Confirmed: false},
		{ID: "another-target", Disposable: true, Confirmed: true},
	} {
		if err := profile.ValidateTarget(target); err == nil {
			t.Fatalf("unsafe target was accepted: %+v", target)
		}
	}
	if err := profile.ValidateTarget(ExperimentTarget{ID: profile.TargetID, Disposable: true, Confirmed: true}); err != nil {
		t.Fatalf("disposable target was rejected: %v", err)
	}

	profile.Enabled = false
	profile.Mode = DelayDisabled
	profile.DelayUS = 0
	profile.Concurrency = 0
	profile.DeadlineMS = 0
	profile.MaxRetries = 0
	profile.RetryError = ""
	if err := profile.ValidateTarget(ExperimentTarget{}); err != nil {
		t.Fatalf("disabled profile required a target gate: %v", err)
	}
}

func TestExperimentArtifactValidation(t *testing.T) {
	artifact := ExperimentArtifact{
		SchemaVersion:                ExperimentSchemaVersion,
		Profile:                      validExperimentProfile(),
		InputIdentitySHA256:          strings.Repeat("a", 64),
		InputOrder:                   "canonical",
		BinaryRevision:               "f96714c03",
		EngineRevision:               "fc68f09a25defb128edfd722ec82696492dbb692",
		Hardware:                     "test-host",
		RuntimeLimits:                "cpu=4,memory=1GiB",
		CacheState:                   "cold",
		Encryption:                   "metadata=aes-gcm",
		WAL:                          "s3-put-completion",
		BatchSize:                    1000,
		EffectiveConcurrency:         4,
		ObservationWindowMS:          60000,
		CompletionCriterion:          "all-records-committed",
		ResultSHA256:                 strings.Repeat("b", 64),
		Outcome:                      "success",
		Completed:                    1,
		SampledDelayUS:               8000,
		ObservedDelayUS:              8100,
		ServerCompletedBeforeTimeout: EvidenceTrue,
		Unknowns:                     []ExperimentUnknown{{Field: "provider_replication_delay", Reason: "provider does not expose it"}},
	}
	encoded, err := json.Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeExperimentArtifact(encoded)
	if err != nil || decoded.Unknowns[0] != artifact.Unknowns[0] {
		t.Fatalf("artifact decode = %#v, %v", decoded, err)
	}
	artifact.ServerCompletedBeforeTimeout = "yes"
	if err := artifact.Validate(); err == nil || !strings.Contains(err.Error(), "server-completion") {
		t.Fatalf("artifact availability error = %v", err)
	}
}

func TestExperimentArtifactRejectsEveryExceededBound(t *testing.T) {
	base := func() ExperimentArtifact {
		return ExperimentArtifact{
			SchemaVersion: ExperimentSchemaVersion, Profile: validExperimentProfile(),
			InputIdentitySHA256: strings.Repeat("a", 64), InputOrder: "canonical",
			BinaryRevision: "binary", EngineRevision: "engine", Hardware: "host", RuntimeLimits: "bounded",
			CacheState: "cold", Encryption: "enabled", WAL: "s3", BatchSize: 1,
			EffectiveConcurrency: 1, ObservationWindowMS: 1, CompletionCriterion: "complete",
			ResultSHA256: strings.Repeat("b", 64), Outcome: "success", Completed: 1,
			ServerCompletedBeforeTimeout: EvidenceTrue,
		}
	}
	tests := []struct {
		name string
		edit func(*ExperimentArtifact)
	}{
		{"window", func(artifact *ExperimentArtifact) { artifact.ObservationWindowMS = MaxExperimentWindowMS + 1 }},
		{"batch", func(artifact *ExperimentArtifact) { artifact.BatchSize = MaxExperimentBatchSize + 1 }},
		{"concurrency", func(artifact *ExperimentArtifact) { artifact.EffectiveConcurrency = 4097 }},
		{"events", func(artifact *ExperimentArtifact) { artifact.Completed = MaxExperimentEvents + 1 }},
		{"sampled-delay", func(artifact *ExperimentArtifact) { artifact.SampledDelayUS = MaxExperimentDelayUS + 1 }},
		{"observed-delay", func(artifact *ExperimentArtifact) { artifact.ObservedDelayUS = MaxExperimentDelayUS + 1 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			artifact := base()
			test.edit(&artifact)
			if err := artifact.Validate(); err == nil {
				t.Fatal("exceeded artifact bound was accepted")
			}
		})
	}
}

func TestExperimentArtifactRejectsContradictoryOutcomeEvidence(t *testing.T) {
	artifact := ExperimentArtifact{
		SchemaVersion: ExperimentSchemaVersion, Profile: validExperimentProfile(),
		InputIdentitySHA256: strings.Repeat("a", 64), InputOrder: "canonical",
		BinaryRevision: "binary", EngineRevision: "engine", Hardware: "host", RuntimeLimits: "bounded",
		CacheState: "cold", Encryption: "enabled", WAL: "s3", BatchSize: 1,
		EffectiveConcurrency: 1, ObservationWindowMS: 1, CompletionCriterion: "complete",
		ResultSHA256: strings.Repeat("b", 64), Outcome: "timeout",
		ServerCompletedBeforeTimeout: EvidenceTrue,
	}
	if err := artifact.Validate(); err == nil || !strings.Contains(err.Error(), "timeout evidence") {
		t.Fatalf("missing timeout evidence error = %v", err)
	}
	artifact.Timeouts = 1
	if err := artifact.Validate(); err != nil {
		t.Fatalf("timeout-after-completion evidence error = %v", err)
	}
	artifact.Outcome = "cancellation"
	artifact.Timeouts = 0
	artifact.ServerCompletedBeforeTimeout = EvidenceUnknown
	if err := artifact.Validate(); err == nil || !strings.Contains(err.Error(), "unfinished work") {
		t.Fatalf("cancellation evidence error = %v", err)
	}
}

func TestExperimentArtifactRejectsDisabledDelayEvidence(t *testing.T) {
	profile := validExperimentProfile()
	profile.Enabled = false
	profile.Mode = DelayDisabled
	profile.DelayUS = 0
	profile.Concurrency = 0
	profile.DeadlineMS = 0
	profile.MaxRetries = 0
	profile.RetryError = ""
	artifact := ExperimentArtifact{
		SchemaVersion: ExperimentSchemaVersion, Profile: profile,
		InputIdentitySHA256: strings.Repeat("a", 64), InputOrder: "canonical",
		BinaryRevision: "binary", EngineRevision: "engine", Hardware: "host", RuntimeLimits: "bounded",
		CacheState: "cold", Encryption: "enabled", WAL: "s3", BatchSize: 1,
		EffectiveConcurrency: 1, ObservationWindowMS: 1, CompletionCriterion: "complete",
		ResultSHA256: strings.Repeat("b", 64), Outcome: "success", Completed: 1,
		ServerCompletedBeforeTimeout: EvidenceTrue,
	}
	artifact.SampledDelayUS = 1
	if err := artifact.Validate(); err == nil || !strings.Contains(err.Error(), "disabled experiment") {
		t.Fatalf("disabled delay evidence error = %v", err)
	}
}

func TestExperimentDecodeRejectsDuplicateAndCaseVariantMembers(t *testing.T) {
	encoded, err := json.Marshal(validExperimentProfile())
	if err != nil {
		t.Fatal(err)
	}
	duplicate := append(encoded[:len(encoded)-1], []byte(`,"test_only":false}`)...)
	if _, err := DecodeExperimentProfile(duplicate); err == nil || !strings.Contains(err.Error(), "duplicate field") {
		t.Fatalf("duplicate member error = %v", err)
	}
	caseVariant := strings.Replace(string(encoded), `"test_only"`, `"Test_Only"`, 1)
	if _, err := DecodeExperimentProfile([]byte(caseVariant)); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("case-variant member error = %v", err)
	}
	nullScalar := strings.Replace(string(encoded), `"delay_us":8000`, `"delay_us":null`, 1)
	if _, err := DecodeExperimentProfile([]byte(nullScalar)); err == nil || !strings.Contains(err.Error(), "null is not allowed") {
		t.Fatalf("null scalar error = %v", err)
	}
	if _, err := DecodeExperimentProfile(append(encoded, []byte(` {}`)...)); err == nil {
		t.Fatalf("trailing JSON error = %v", err)
	}
}

func TestExperimentProfileRejectsIncompatibleBackendBoundary(t *testing.T) {
	profile := validExperimentProfile()
	profile.Scenario = "nfs_hdd"
	profile.Backend = "nfs_hdd"
	profile.Role = "repository"
	profile.Method = "multipart_complete"
	profile.Acknowledgement = "replicated_object_commit"
	if err := profile.Validate(); err == nil || !strings.Contains(err.Error(), "incompatible") {
		t.Fatalf("backend compatibility error = %v", err)
	}
}

func TestExperimentProfileRejectsNonPersistentDurabilityForEveryRole(t *testing.T) {
	for _, role := range []string{"rpc", "cache", "scratch"} {
		t.Run(role, func(t *testing.T) {
			profile := validExperimentProfile()
			profile.Mode = DelayDurability
			profile.Latency = LatencyDurabilityCompletion
			profile.Placement = "inside_service"
			profile.Role = role
			profile.Method = "read"
			profile.Acknowledgement = "applied"
			if err := profile.Validate(); err == nil || !strings.Contains(err.Error(), "persistence operation") {
				t.Fatalf("durability role bypass error = %v", err)
			}
		})
	}
}

func TestExperimentProfileRejectsBackendAcknowledgementForWrongScenarioRole(t *testing.T) {
	profile := validExperimentProfile()
	profile.Scenario = "nfs_hdd"
	profile.Backend = "nfs_hdd"
	profile.Role = "scratch"
	profile.Method = "put"
	profile.Acknowledgement = "replicated_object_commit"
	if err := profile.Validate(); err == nil || !strings.Contains(err.Error(), "incompatible with scenario") {
		t.Fatalf("backend acknowledgement compatibility error = %v", err)
	}
}

func TestExperimentProfileRejectsWriteAcknowledgementForReadMethod(t *testing.T) {
	for _, test := range []struct {
		scenario, method, acknowledgement string
	}{
		{scenario: "s3", method: "get", acknowledgement: "multipart_completion"},
		{scenario: "rados_three_replica", method: "get", acknowledgement: "replicated_object_commit"},
	} {
		profile := validExperimentProfile()
		profile.Scenario = test.scenario
		profile.Backend = test.scenario
		profile.Role = "rpc"
		profile.Method = test.method
		profile.Acknowledgement = test.acknowledgement
		if err := profile.Validate(); err == nil || !strings.Contains(err.Error(), "incompatible with method") {
			t.Fatalf("%s read acknowledgement error = %v", test.scenario, err)
		}
	}
}

func TestExperimentProfileRejectsRoleMethodAndDurabilityBoundaryBypass(t *testing.T) {
	profile := validExperimentProfile()
	profile.Scenario = "nfs_hdd"
	profile.Backend = "nfs_hdd"
	profile.Method = "multipart_complete"
	profile.Acknowledgement = "unknown"
	if err := profile.Validate(); err == nil || !strings.Contains(err.Error(), "incompatible with role") {
		t.Fatalf("role method error = %v", err)
	}

	profile = validExperimentProfile()
	profile.Mode = DelayDurability
	profile.Latency = LatencyDurabilityCompletion
	profile.Placement = "inside_service"
	profile.Method = "commit"
	profile.Acknowledgement = "sync_stable_storage"
	if err := profile.Validate(); err == nil || !strings.Contains(err.Error(), "storage dependency role") {
		t.Fatalf("durability boundary error = %v", err)
	}
}

func TestExperimentProfileEnforcesCompleteRoleAndAcknowledgementMatrix(t *testing.T) {
	profile := validExperimentProfile()
	profile.Role = "source"
	profile.Method = "commit"
	if err := profile.Validate(); err == nil || !strings.Contains(err.Error(), "incompatible with role") {
		t.Fatalf("source method error = %v", err)
	}

	profile = validExperimentProfile()
	profile.Method = "get"
	if err := profile.Validate(); err == nil || !strings.Contains(err.Error(), "incompatible with method") {
		t.Fatalf("read acknowledgement error = %v", err)
	}
}

func TestExperimentProfileSeparatesScenarioFromTargetBackend(t *testing.T) {
	profile := validExperimentProfile()
	profile.Scenario = "s3"
	profile.Backend = "nfs_hdd"
	profile.Role = "source"
	profile.Method = "stat"
	profile.Acknowledgement = "unknown"
	if err := profile.Validate(); err != nil {
		t.Fatalf("mixed-resource profile error = %v", err)
	}
}

func TestExperimentProfileRejectsEveryExceededBound(t *testing.T) {
	tests := []struct {
		name string
		edit func(*ExperimentProfile)
	}{
		{"delay", func(profile *ExperimentProfile) { profile.DelayUS = MaxExperimentDelayUS + 1 }},
		{"jitter", func(profile *ExperimentProfile) { profile.JitterUS = MaxExperimentDelayUS + 1 }},
		{"bandwidth", func(profile *ExperimentProfile) { profile.BandwidthBPS = MaxExperimentBandwidth + 1 }},
		{"concurrency", func(profile *ExperimentProfile) { profile.Concurrency = 4097 }},
		{"deadline", func(profile *ExperimentProfile) { profile.DeadlineMS = MaxExperimentWindowMS + 1 }},
		{"retries", func(profile *ExperimentProfile) { profile.MaxRetries = 1001 }},
		{"holds", func(profile *ExperimentProfile) { profile.Holds = make([]string, MaxExperimentDimensions+1) }},
		{"unknowns", func(profile *ExperimentProfile) {
			profile.Unknowns = make([]ExperimentUnknown, MaxExperimentUnknowns+1)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			profile := validExperimentProfile()
			test.edit(&profile)
			if err := profile.Validate(); err == nil {
				t.Fatal("exceeded bound was accepted")
			}
		})
	}
}
