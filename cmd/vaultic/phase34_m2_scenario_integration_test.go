package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/fs"
	"github.com/otuschhoff/vaultic/internal/telemetry"
)

var phase34M2ResponseSweep = []time.Duration{0, time.Millisecond, 8 * time.Millisecond, 25 * time.Millisecond, 100 * time.Millisecond, 250 * time.Millisecond}

func phase34M2Profile(action, role, method string, delay time.Duration) telemetry.ExperimentProfile {
	return telemetry.ExperimentProfile{
		SchemaVersion: telemetry.ExperimentSchemaVersion, ProfileID: fmt.Sprintf("m2-%s-%s-%d", action, method, delay.Milliseconds()),
		Enabled: true, TestOnly: true, Scenario: "local", Backend: "local", Mode: telemetry.DelayAcknowledgement,
		Operation: action, Role: role, Method: method, AccessPattern: "sequential",
		TargetID: "disposable-action-fixture", ResourceID: "local-fixture-device",
		Placement: "after_completion", Latency: telemetry.LatencyAcknowledgementWindow,
		Interpretation: "additive", Endpoint: "caller", Acknowledgement: "unknown",
		DelayUS: uint64(delay / time.Microsecond), Concurrency: 1, Seed: 34,
		Holds: []string{"none"},
	}
}

func phase34M2Controller(t *testing.T, profile telemetry.ExperimentProfile) *telemetry.ExperimentController {
	t.Helper()
	controller, err := telemetry.NewScenarioHarness().Controller(profile, telemetry.ExperimentTarget{
		ID: "disposable-action-fixture", Disposable: true, Confirmed: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return controller
}

func phase34M2Artifact(t *testing.T, directory string, profile telemetry.ExperimentProfile, observation telemetry.ExperimentObservation, elapsed time.Duration, inputIdentity, result string) {
	t.Helper()
	inputDigest := sha256.Sum256([]byte("input\x00" + inputIdentity))
	resultDigest := sha256.Sum256([]byte("result\x00" + result))
	artifact := telemetry.ExperimentArtifact{
		SchemaVersion: telemetry.ExperimentSchemaVersion, Profile: profile,
		InputIdentitySHA256: hex.EncodeToString(inputDigest[:]), InputOrder: "canonical",
		BinaryRevision: "test-worktree", EngineRevision: "fc68f09a25defb128edfd722ec82696492dbb692",
		Hardware: runtime.GOOS + "/" + runtime.GOARCH, RuntimeLimits: "single-fixture-worker",
		CacheState: "unknown", Encryption: "test-default", WAL: "test-default",
		BatchSize: 1, EffectiveConcurrency: 1, ObservationWindowMS: uint64(max(elapsed.Milliseconds(), 1)),
		CompletionCriterion: "action correctness oracle passed", ResultSHA256: hex.EncodeToString(resultDigest[:]),
		Outcome: "success", Completed: max(observation.Completed, 1),
		SampledDelayUS: observation.SampledDelayUS, ObservedDelayUS: observation.ObservedDelayUS,
		ServerCompletedBeforeTimeout: telemetry.EvidenceTrue,
		Unknowns:                     []telemetry.ExperimentUnknown{{Field: "throughput", Reason: "small correctness fixture is not throughput evidence"}},
	}
	if err := artifact.Validate(); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.MarshalIndent(artifact, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("%s-%s-%d.json", profile.Operation, profile.Method, time.Now().UnixNano())
	if err := os.WriteFile(filepath.Join(directory, name), encoded, 0o600); err != nil {
		t.Fatal(err)
	}
}

func phase34M2Sweep(t *testing.T, run func(*testing.T, time.Duration, int)) {
	t.Helper()
	for _, delay := range phase34M2ResponseSweep {
		for repeat := range 3 {
			t.Run(fmt.Sprintf("%dms/repeat-%d", delay.Milliseconds(), repeat+1), func(t *testing.T) {
				run(t, delay, repeat)
			})
		}
	}
}

func TestPhase34M2BackupSourceReadSweep(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	testRunInit(t, env.globalOptions)
	source := filepath.Join(env.base, "m2-backup-source")
	if err := os.MkdirAll(source, 0o700); err != nil {
		t.Fatal(err)
	}
	content := []byte("phase-34-m2-backup-content")
	if err := os.WriteFile(filepath.Join(source, "content"), content, 0o600); err != nil {
		t.Fatal(err)
	}
	artifacts := t.TempDir()
	count := 0
	phase34M2Sweep(t, func(t *testing.T, delay time.Duration, repeat int) {
		content = fmt.Appendf(nil, "phase-34-m2-backup-content-%d-%d", delay, repeat)
		if err := os.WriteFile(filepath.Join(source, "content"), content, 0o600); err != nil {
			t.Fatal(err)
		}
		profile := phase34M2Profile("backup", "source", "read", delay)
		controller := phase34M2Controller(t, profile)
		options := backupOptions{Host: "phase34-m2"}
		options.FSTestHook = func(filesystem fs.FS) fs.FS {
			return telemetry.WrapExperimentFS(context.Background(), filesystem, controller)
		}
		started := time.Now()
		testRunBackup(t, "", []string{source}, options, env.globalOptions)
		count++
		testListSnapshots(t, env.globalOptions, count)
		observation := controller.Observation()
		if observation.Completed == 0 || observation.Active != 0 {
			t.Fatalf("backup source observation = %+v", observation)
		}
		phase34M2Artifact(t, artifacts, profile, observation, time.Since(started), string(content), fmt.Sprintf("snapshot-count=%d", count))
	})
	restore := filepath.Join(env.base, "m2-backup-restore")
	testRunRestore(t, env.globalOptions, restore, "latest:"+toPathInSnapshot(filepath.Dir(source)))
	if diff := directoriesContentsDiff(t, source, filepath.Join(restore, filepath.Base(source))); diff != "" {
		t.Fatalf("backup sweep restore differs: %s", diff)
	}
}

func TestPhase34M2RestoreRangeReadSweep(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	testRunInit(t, env.globalOptions)
	source := filepath.Join(env.base, "m2-restore-source")
	if err := os.MkdirAll(source, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "content"), []byte("phase-34-m2-restore-content"), 0o600); err != nil {
		t.Fatal(err)
	}
	testRunBackup(t, "", []string{source}, backupOptions{Host: "phase34-m2"}, env.globalOptions)
	artifacts := t.TempDir()
	phase34M2Sweep(t, func(t *testing.T, delay time.Duration, repeat int) {
		profile := phase34M2Profile("restore", "repository", "range_get", delay)
		controller := phase34M2Controller(t, profile)
		options := env.globalOptions
		options.BackendInnerTestHook = func(inner backend.Backend) (backend.Backend, error) {
			return telemetry.WrapExperimentBackend(inner, controller), nil
		}
		target := filepath.Join(env.base, fmt.Sprintf("m2-restore-%d-%d", delay.Milliseconds(), repeat))
		started := time.Now()
		testRunRestore(t, options, target, "latest:"+toPathInSnapshot(filepath.Dir(source)))
		if diff := directoriesContentsDiff(t, source, filepath.Join(target, filepath.Base(source))); diff != "" {
			t.Fatalf("restore sweep differs: %s", diff)
		}
		observation := controller.Observation()
		if observation.Completed == 0 || observation.Active != 0 {
			t.Fatalf("restore range-read observation = %+v", observation)
		}
		phase34M2Artifact(t, artifacts, profile, observation, time.Since(started), "phase-34-m2-restore-content", "restored-content-match")
	})
}

func TestPhase34M2ForgetDeleteSweep(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	testRunInit(t, env.globalOptions)
	source := filepath.Join(env.base, "m2-forget-source")
	if err := os.MkdirAll(source, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "content"), []byte("retained"), 0o600); err != nil {
		t.Fatal(err)
	}
	testRunBackup(t, "", []string{source}, backupOptions{Host: "retained"}, env.globalOptions)
	artifacts := t.TempDir()
	phase34M2Sweep(t, func(t *testing.T, delay time.Duration, repeat int) {
		if err := os.WriteFile(filepath.Join(source, "temporary"), fmt.Appendf(nil, "%d-%d", delay, repeat), 0o600); err != nil {
			t.Fatal(err)
		}
		testRunBackup(t, "", []string{source}, backupOptions{Host: "discard"}, env.globalOptions)
		snapshots := testListSnapshots(t, env.globalOptions, 2)
		profile := phase34M2Profile("forget", "repository", "delete", delay)
		controller := phase34M2Controller(t, profile)
		options := env.globalOptions
		options.BackendInnerTestHook = func(inner backend.Backend) (backend.Backend, error) {
			return telemetry.WrapExperimentBackend(inner, controller), nil
		}
		started := time.Now()
		testRunForget(t, options, forgetOptions{}, snapshots[0].String())
		testListSnapshots(t, env.globalOptions, 1)
		observation := controller.Observation()
		if observation.Completed == 0 || observation.Active != 0 {
			t.Fatalf("forget delete observation = %+v", observation)
		}
		phase34M2Artifact(t, artifacts, profile, observation, time.Since(started), snapshots[0].String(), "one-snapshot-retained")
	})
}

func TestPhase34M2PruneDeleteSweep(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	testRunInit(t, env.globalOptions)
	source := filepath.Join(env.base, "m2-prune-source")
	if err := os.MkdirAll(source, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "content"), []byte("retained"), 0o600); err != nil {
		t.Fatal(err)
	}
	testRunBackup(t, "", []string{source}, backupOptions{Host: "retained"}, env.globalOptions)
	artifacts := t.TempDir()
	phase34M2Sweep(t, func(t *testing.T, delay time.Duration, repeat int) {
		if err := os.WriteFile(filepath.Join(source, "temporary"), fmt.Appendf(nil, "unused-%d-%d", delay, repeat), 0o600); err != nil {
			t.Fatal(err)
		}
		testRunBackup(t, "", []string{source}, backupOptions{Host: "discard"}, env.globalOptions)
		snapshots := testListSnapshots(t, env.globalOptions, 2)
		testRunForget(t, env.globalOptions, forgetOptions{}, snapshots[0].String())
		profile := phase34M2Profile("prune", "repository", "delete", delay)
		controller := phase34M2Controller(t, profile)
		options := env.globalOptions
		options.BackendInnerTestHook = func(inner backend.Backend) (backend.Backend, error) {
			return telemetry.WrapExperimentBackend(inner, controller), nil
		}
		started := time.Now()
		testRunPrune(t, options, pruneOptions{MaxUnused: "0%"})
		testListSnapshots(t, env.globalOptions, 1)
		testRunCheck(t, env.globalOptions)
		observation := controller.Observation()
		if observation.Completed == 0 || observation.Active != 0 {
			t.Fatalf("prune delete observation = %+v", observation)
		}
		phase34M2Artifact(t, artifacts, profile, observation, time.Since(started), snapshots[0].String(), "retained-snapshot-check-clean")
	})
}
