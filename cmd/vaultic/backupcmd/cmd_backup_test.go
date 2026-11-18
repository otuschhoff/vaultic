package backupcmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/archiver"
	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/errors"
	"github.com/otuschhoff/vaultic/internal/fs"
	"github.com/otuschhoff/vaultic/internal/global"
	enginepkg "github.com/otuschhoff/vaultic/internal/index"
	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/reconcile"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/telemetry"
	rtest "github.com/otuschhoff/vaultic/internal/test"
	"github.com/otuschhoff/vaultic/internal/ui"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

type backupCloseEngine struct {
	*enginepkg.LegacyEngine
	close func() error
	stats *enginepkg.BlobLookupStats
}

func TestBackupSnapshotModeValidation(t *testing.T) {
	for _, test := range []struct {
		name      string
		options   backupOptions
		wantError bool
	}{
		{name: "default"},
		{name: "wide-publication", options: backupOptions{MetadataPublicationGroupSize: 64}},
		{name: "negative-publication", options: backupOptions{MetadataPublicationGroupSize: -1}, wantError: true},
		{name: "oversized-publication", options: backupOptions{MetadataPublicationGroupSize: 65}, wantError: true},
		{name: "per-source", options: backupOptions{SnapshotPerSource: true}},
		{name: "coalesced", options: backupOptions{CoalesceSources: true}},
		{name: "conflict", options: backupOptions{SnapshotPerSource: true, CoalesceSources: true}, wantError: true},
		{name: "stdin", options: backupOptions{SnapshotPerSource: true, Stdin: true}, wantError: true},
		{name: "command", options: backupOptions{SnapshotPerSource: true, StdinCommand: true}, wantError: true},
		{name: "catalog-without-on-demand", options: backupOptions{MetadataExactCatalog: true}, wantError: true},
		{name: "catalog", options: backupOptions{MetadataExactCatalog: true, MetadataOnDemand: true, MetadataScratch: "scratch", MetadataCacheMiB: 64}},
		{name: "batch-without-on-demand", options: backupOptions{MetadataBatchDelay: time.Millisecond}, wantError: true},
		{name: "batch", options: backupOptions{MetadataBatchDelay: time.Millisecond, MetadataOnDemand: true, MetadataScratch: "scratch", MetadataCacheMiB: 64}},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := test.options.Check(global.Options{}, []string{"source"})
			if (err != nil) != test.wantError {
				t.Fatalf("Check() = %v, wantError %t", err, test.wantError)
			}
		})
	}
}

func (engine *backupCloseEngine) Close() error { return engine.close() }

func (engine *backupCloseEngine) BlobLookupStats() (enginepkg.BlobLookupStats, bool) {
	if engine.stats == nil {
		return enginepkg.BlobLookupStats{}, false
	}
	return *engine.stats, true
}

func TestBackupCloseReportsLookupStatsAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	term := &ui.MockTerminal{}
	repo := repository.TestRepository(t)
	stats := &enginepkg.BlobLookupStats{Capacity: 2}
	repo.SetEngine(&backupCloseEngine{LegacyEngine: enginepkg.NewLegacyEngine(), stats: stats, close: func() error {
		if len(term.Output) != 0 {
			t.Fatal("stats emitted before engine close")
		}
		stats.SizeRPCs = 7
		return nil
	}})
	run := &backupRun{ctx: ctx, repo: repo, term: term, globalOptions: global.Options{JSON: true}}
	run.close()
	if len(term.Output) != 1 {
		t.Fatalf("output=%v", term.Output)
	}
	var record struct {
		MessageType string `json:"message_type"`
		enginepkg.BlobLookupStats
	}
	if err := json.Unmarshal([]byte(term.Output[0]), &record); err != nil {
		t.Fatal(err)
	}
	if record.MessageType != "metadata_lookup_stats" || record.Capacity != 2 || record.SizeRPCs != 7 {
		t.Fatalf("invalid final stats: %+v", record)
	}
}

func TestBackupEncryptionReadDelta(test *testing.T) {
	baseline := &daemon.EncryptionReadMetrics{Reads: 4, CiphertextOriginNanoseconds: 500, CryptoQueueNanoseconds: 10}
	current := &daemon.EncryptionReadMetrics{Reads: 6, CiphertextOriginNanoseconds: 900, CryptoQueueNanoseconds: 30}
	actual := backupEncryptionReadDelta(current, baseline)
	if actual == nil || actual.Reads != 2 || actual.CiphertextOriginNanoseconds != 400 || actual.CryptoQueueNanoseconds != 20 || current.Reads != 6 || baseline.Reads != 4 {
		test.Fatalf("invalid or destructive main-interval delta: %+v", actual)
	}
	if backupEncryptionReadDelta(nil, baseline) != nil || backupEncryptionReadDelta(current, nil) != nil {
		test.Fatal("unavailable telemetry must not become a zero baseline")
	}
	current.CryptoQueueNanoseconds = 1
	if backupEncryptionReadDelta(current, baseline) != nil {
		test.Fatal("counter reset must not expose a partial interval")
	}
	current.CryptoQueueNanoseconds = 30
	for _, field := range []struct {
		baseline *uint64
		current  *uint64
		read     func(*daemon.EncryptionReadMetrics) uint64
	}{
		{&baseline.CiphertextOriginRequestNS, &current.CiphertextOriginRequestNS, func(value *daemon.EncryptionReadMetrics) uint64 { return value.CiphertextOriginRequestNS }},
		{&baseline.CiphertextOriginBodyNS, &current.CiphertextOriginBodyNS, func(value *daemon.EncryptionReadMetrics) uint64 { return value.CiphertextOriginBodyNS }},
		{&baseline.CiphertextOriginBodyCalls, &current.CiphertextOriginBodyCalls, func(value *daemon.EncryptionReadMetrics) uint64 { return value.CiphertextOriginBodyCalls }},
		{&baseline.CiphertextOriginRequestSuccesses, &current.CiphertextOriginRequestSuccesses, func(value *daemon.EncryptionReadMetrics) uint64 { return value.CiphertextOriginRequestSuccesses }},
		{&baseline.CiphertextOriginRequestNotFound, &current.CiphertextOriginRequestNotFound, func(value *daemon.EncryptionReadMetrics) uint64 { return value.CiphertextOriginRequestNotFound }},
		{&baseline.CiphertextOriginRequestErrors, &current.CiphertextOriginRequestErrors, func(value *daemon.EncryptionReadMetrics) uint64 { return value.CiphertextOriginRequestErrors }},
		{&baseline.CiphertextOriginRequestCancellations, &current.CiphertextOriginRequestCancellations, func(value *daemon.EncryptionReadMetrics) uint64 { return value.CiphertextOriginRequestCancellations }},
		{&baseline.CiphertextOriginIdentityRejections, &current.CiphertextOriginIdentityRejections, func(value *daemon.EncryptionReadMetrics) uint64 { return value.CiphertextOriginIdentityRejections }},
		{&baseline.CiphertextOriginBodySuccesses, &current.CiphertextOriginBodySuccesses, func(value *daemon.EncryptionReadMetrics) uint64 { return value.CiphertextOriginBodySuccesses }},
		{&baseline.CiphertextOriginBodyErrors, &current.CiphertextOriginBodyErrors, func(value *daemon.EncryptionReadMetrics) uint64 { return value.CiphertextOriginBodyErrors }},
		{&baseline.CiphertextOriginBodyCancellations, &current.CiphertextOriginBodyCancellations, func(value *daemon.EncryptionReadMetrics) uint64 { return value.CiphertextOriginBodyCancellations }},
		{&baseline.HeaderValidationCalls, &current.HeaderValidationCalls, func(value *daemon.EncryptionReadMetrics) uint64 { return value.HeaderValidationCalls }},
		{&baseline.HeaderValidationNanoseconds, &current.HeaderValidationNanoseconds, func(value *daemon.EncryptionReadMetrics) uint64 { return value.HeaderValidationNanoseconds }},
		{&baseline.HeaderValidationCoalescedReads, &current.HeaderValidationCoalescedReads, func(value *daemon.EncryptionReadMetrics) uint64 { return value.HeaderValidationCoalescedReads }},
		{&baseline.ChunkCacheHits, &current.ChunkCacheHits, func(value *daemon.EncryptionReadMetrics) uint64 { return value.ChunkCacheHits }},
		{&baseline.ChunkCacheMisses, &current.ChunkCacheMisses, func(value *daemon.EncryptionReadMetrics) uint64 { return value.ChunkCacheMisses }},
		{&baseline.ChunkCacheEvictions, &current.ChunkCacheEvictions, func(value *daemon.EncryptionReadMetrics) uint64 { return value.ChunkCacheEvictions }},
		{&baseline.ChunkCoalescedReads, &current.ChunkCoalescedReads, func(value *daemon.EncryptionReadMetrics) uint64 { return value.ChunkCoalescedReads }},
		{&baseline.ChunkSampledFirstFetches, &current.ChunkSampledFirstFetches, func(value *daemon.EncryptionReadMetrics) uint64 { return value.ChunkSampledFirstFetches }},
		{&baseline.ChunkSampledRefetches, &current.ChunkSampledRefetches, func(value *daemon.EncryptionReadMetrics) uint64 { return value.ChunkSampledRefetches }},
		{&baseline.ChunkSampledHistoryEvictions, &current.ChunkSampledHistoryEvictions, func(value *daemon.EncryptionReadMetrics) uint64 { return value.ChunkSampledHistoryEvictions }},
		{&baseline.ChunkSampledReuseNS, &current.ChunkSampledReuseNS, func(value *daemon.EncryptionReadMetrics) uint64 { return value.ChunkSampledReuseNS }},
		{&baseline.ChunkSampledReuseBytes, &current.ChunkSampledReuseBytes, func(value *daemon.EncryptionReadMetrics) uint64 { return value.ChunkSampledReuseBytes }},
		{&baseline.ChunkSampledRefetchWithin64M, &current.ChunkSampledRefetchWithin64M, func(value *daemon.EncryptionReadMetrics) uint64 { return value.ChunkSampledRefetchWithin64M }},
		{&baseline.ChunkSampledRefetchWithin256M, &current.ChunkSampledRefetchWithin256M, func(value *daemon.EncryptionReadMetrics) uint64 { return value.ChunkSampledRefetchWithin256M }},
		{&baseline.ChunkSampledRefetchWithin1G, &current.ChunkSampledRefetchWithin1G, func(value *daemon.EncryptionReadMetrics) uint64 { return value.ChunkSampledRefetchWithin1G }},
		{&baseline.ReadWindowsDropped, &current.ReadWindowsDropped, func(value *daemon.EncryptionReadMetrics) uint64 { return value.ReadWindowsDropped }},
	} {
		*field.baseline, *field.current = 5, 9
		actual := backupEncryptionReadDelta(current, baseline)
		if actual == nil || field.read(actual) != 4 {
			test.Fatal("chunk counter was not differenced")
		}
		*field.current = 1
		if backupEncryptionReadDelta(current, baseline) != nil {
			test.Fatal("chunk counter reset became a valid interval")
		}
		*field.baseline, *field.current = 0, 0
	}
	baseline.CiphertextOriginRequestActive, baseline.CiphertextOriginBodyActive = 5, 6
	current.CiphertextOriginRequestActive, current.CiphertextOriginBodyActive = 1, 2
	actual = backupEncryptionReadDelta(current, baseline)
	if actual == nil || actual.CiphertextOriginRequestActive != 1 || actual.CiphertextOriginBodyActive != 2 {
		test.Fatal("origin active gauges were differenced as monotonic counters")
	}
}

func TestBackupEncryptionOriginErrorDetailsDelta(test *testing.T) {
	key, added := "header/not_found/plain/io_not_found", "chunk/precondition/conditional/io_unclassified"
	baseline := &daemon.EncryptionReadMetrics{CiphertextOriginRequestErrorCounts: map[string]uint64{"dropped": 2, key: 5}}
	current := &daemon.EncryptionReadMetrics{CiphertextOriginRequestErrorCounts: map[string]uint64{"dropped": 3, key: 8, added: 4}}
	actual := backupEncryptionReadDelta(current, baseline)
	if actual == nil || !reflect.DeepEqual(actual.CiphertextOriginRequestErrorCounts, map[string]uint64{"dropped": 1, key: 3, added: 4}) {
		test.Fatalf("invalid error detail delta: %+v", actual)
	}
	actual.CiphertextOriginRequestErrorCounts[key] = 100
	if current.CiphertextOriginRequestErrorCounts[key] != 8 || baseline.CiphertextOriginRequestErrorCounts[key] != 5 {
		test.Fatal("delta aliased source maps")
	}
	for _, value := range []uint64{1, 0} {
		current.CiphertextOriginRequestErrorCounts[key] = value
		if backupEncryptionReadDelta(current, baseline) != nil {
			test.Fatal("detail counter reset accepted")
		}
	}
	delete(current.CiphertextOriginRequestErrorCounts, key)
	if backupEncryptionReadDelta(current, baseline) != nil {
		test.Fatal("disappeared detail category accepted")
	}
	baseline.CiphertextOriginRequestErrorCounts = nil
	actual = backupEncryptionReadDelta(current, baseline)
	if actual == nil || actual.CiphertextOriginRequestErrorCounts != nil {
		test.Fatal("missing baseline details became measured zeros")
	}
	baseline.CiphertextOriginRequestErrorCounts, current.CiphertextOriginRequestErrorCounts = current.CiphertextOriginRequestErrorCounts, nil
	actual = backupEncryptionReadDelta(current, baseline)
	if actual == nil || actual.CiphertextOriginRequestErrorCounts != nil {
		test.Fatal("missing current details became measured zeros")
	}
}

func TestBackupEncryptionOriginBodyPayloadsDelta(test *testing.T) {
	baseline := &daemon.EncryptionReadMetrics{CiphertextOriginBodyPayloads: &daemon.EncryptionOriginBodyPayloads{
		FileCalls: 2, FileNS: 3, StreamCalls: 5, StreamNS: 7,
	}}
	current := &daemon.EncryptionReadMetrics{CiphertextOriginBodyPayloads: &daemon.EncryptionOriginBodyPayloads{
		FileCalls: 13, FileNS: 20, StreamCalls: 28, StreamNS: 36,
	}}
	delta := backupEncryptionReadDelta(current, baseline)
	want := daemon.EncryptionOriginBodyPayloads{FileCalls: 11, FileNS: 17, StreamCalls: 23, StreamNS: 29}
	if delta == nil || delta.CiphertextOriginBodyPayloads == nil || *delta.CiphertextOriginBodyPayloads != want {
		test.Fatal("body payload delta differs")
	}
	delta.CiphertextOriginBodyPayloads.FileCalls = 99
	if current.CiphertextOriginBodyPayloads.FileCalls != 13 || baseline.CiphertextOriginBodyPayloads.FileCalls != 2 {
		test.Fatal("body payload delta aliases input")
	}
	for _, reset := range []*uint64{&current.CiphertextOriginBodyPayloads.FileCalls, &current.CiphertextOriginBodyPayloads.FileNS,
		&current.CiphertextOriginBodyPayloads.StreamCalls, &current.CiphertextOriginBodyPayloads.StreamNS} {
		original := *reset
		*reset = 0
		if backupEncryptionReadDelta(current, baseline) != nil {
			test.Fatal("body payload counter reset accepted")
		}
		*reset = original
	}
	legacy := &daemon.EncryptionReadMetrics{}
	for _, pair := range [][2]*daemon.EncryptionReadMetrics{{current, legacy}, {legacy, baseline}} {
		if result := backupEncryptionReadDelta(pair[0], pair[1]); result == nil || result.CiphertextOriginBodyPayloads != nil {
			test.Fatal("missing body payload boundary became measured zero")
		}
	}
}

func TestBackupEncryptionOriginFileReadDelta(test *testing.T) {
	first, last, want := daemon.EncryptionOriginFileRead{}, daemon.EncryptionOriginFileRead{}, daemon.EncryptionOriginFileRead{}
	for index := 0; index < reflect.TypeOf(first).NumField(); index++ {
		if reflect.TypeOf(first).Field(index).Name == "Active" {
			continue
		}
		reflect.ValueOf(&first).Elem().Field(index).SetUint(3)
		reflect.ValueOf(&last).Elem().Field(index).SetUint(8)
		reflect.ValueOf(&want).Elem().Field(index).SetUint(5)
	}
	last.Active, want.Active = 2, 2
	baseline := &daemon.EncryptionReadMetrics{CiphertextOriginFileRead: &first}
	current := &daemon.EncryptionReadMetrics{CiphertextOriginFileRead: &last}
	delta := backupEncryptionReadDelta(current, baseline)
	if delta == nil || delta.CiphertextOriginFileRead == nil || *delta.CiphertextOriginFileRead != want {
		test.Fatal("file read delta or end active gauge differs")
	}
	delta.CiphertextOriginFileRead.Calls = 99
	if first.Calls != 3 || last.Calls != 8 {
		test.Fatal("file read delta aliases input")
	}
	for index := 0; index < reflect.TypeOf(last).NumField(); index++ {
		if reflect.TypeOf(last).Field(index).Name == "Active" {
			continue
		}
		field := reflect.ValueOf(&last).Elem().Field(index)
		field.SetUint(0)
		if backupEncryptionReadDelta(current, baseline) != nil {
			test.Fatalf("file read counter reset accepted: %s", reflect.TypeOf(last).Field(index).Name)
		}
		field.SetUint(8)
	}
	first.Active = 1
	if backupEncryptionReadDelta(current, baseline) != nil {
		test.Fatal("unfinished baseline file work accepted")
	}
	first.Active = 0
	legacy := &daemon.EncryptionReadMetrics{}
	for _, pair := range [][2]*daemon.EncryptionReadMetrics{{current, legacy}, {legacy, baseline}} {
		if actual := backupEncryptionReadDelta(pair[0], pair[1]); actual == nil || actual.CiphertextOriginFileRead != nil {
			test.Fatal("missing file read boundary became a measured zero")
		}
	}
}

func TestBackupEncryptionOriginFileRangeReadDelta(test *testing.T) {
	first, last, want := daemon.EncryptionOriginFileRangeRead{}, daemon.EncryptionOriginFileRangeRead{}, daemon.EncryptionOriginFileRangeRead{}
	for index := 0; index < reflect.TypeOf(first).NumField(); index++ {
		if reflect.TypeOf(first).Field(index).Name == "Active" {
			continue
		}
		reflect.ValueOf(&first).Elem().Field(index).SetUint(3)
		reflect.ValueOf(&last).Elem().Field(index).SetUint(8)
		reflect.ValueOf(&want).Elem().Field(index).SetUint(5)
	}
	last.Active, want.Active = 2, 2
	baseline := &daemon.EncryptionReadMetrics{CiphertextOriginFileRangeRead: &first}
	current := &daemon.EncryptionReadMetrics{CiphertextOriginFileRangeRead: &last}
	delta := backupEncryptionReadDelta(current, baseline)
	if delta == nil || delta.CiphertextOriginFileRangeRead == nil || *delta.CiphertextOriginFileRangeRead != want {
		test.Fatal("range delta or end active gauge differs")
	}
	delta.CiphertextOriginFileRangeRead.Calls = 99
	if first.Calls != 3 || last.Calls != 8 {
		test.Fatal("range delta aliases inputs")
	}
	for index := 0; index < reflect.TypeOf(last).NumField(); index++ {
		if reflect.TypeOf(last).Field(index).Name == "Active" {
			continue
		}
		field := reflect.ValueOf(&last).Elem().Field(index)
		field.SetUint(0)
		if backupEncryptionReadDelta(current, baseline) != nil {
			test.Fatalf("range reset accepted: %s", reflect.TypeOf(last).Field(index).Name)
		}
		field.SetUint(8)
	}
	first.Active = 1
	if backupEncryptionReadDelta(current, baseline) != nil {
		test.Fatal("unfinished range baseline accepted")
	}
	first.Active = 0
	legacy := &daemon.EncryptionReadMetrics{}
	for _, pair := range [][2]*daemon.EncryptionReadMetrics{{current, legacy}, {legacy, baseline}} {
		if result := backupEncryptionReadDelta(pair[0], pair[1]); result == nil || result.CiphertextOriginFileRangeRead != nil {
			test.Fatal("missing range boundary became measured zero")
		}
	}
}

func TestBackupEncryptionOriginNotModifiedDelta(test *testing.T) {
	first, last := uint64(3), uint64(8)
	baseline := &daemon.EncryptionReadMetrics{CiphertextOriginRequestNotModified: &first}
	current := &daemon.EncryptionReadMetrics{CiphertextOriginRequestNotModified: &last}
	actual := backupEncryptionReadDelta(current, baseline)
	if actual == nil || actual.CiphertextOriginRequestNotModified == nil || *actual.CiphertextOriginRequestNotModified != 5 {
		test.Fatalf("invalid conditional count delta: %+v", actual)
	}
	*actual.CiphertextOriginRequestNotModified = 100
	if first != 3 || last != 8 {
		test.Fatal("delta aliased source counters")
	}
	last = 1
	if backupEncryptionReadDelta(current, baseline) != nil {
		test.Fatal("conditional counter reset accepted")
	}
	for _, boundary := range []**uint64{&current.CiphertextOriginRequestNotModified, &baseline.CiphertextOriginRequestNotModified} {
		last = 8
		previous := *boundary
		*boundary = nil
		actual = backupEncryptionReadDelta(current, baseline)
		if actual == nil || actual.CiphertextOriginRequestNotModified != nil {
			test.Fatal("legacy boundary became a measured zero")
		}
		*boundary = previous
	}
}

func TestBackupEncryptionLatencyDelta(test *testing.T) {
	for _, name := range []string{"CiphertextOriginLatencyBuckets", "ChunkWaitLatencyBuckets", "ChunkAdmissionLatencyBuckets"} {
		baseline, current := &daemon.EncryptionReadMetrics{}, &daemon.EncryptionReadMetrics{}
		first, last := &[8]uint64{1, 2, 3, 4, 5, 6, 7, 8}, &[8]uint64{2, 4, 6, 8, 10, 12, 14, 16}
		reflect.ValueOf(baseline).Elem().FieldByName(name).Set(reflect.ValueOf(first))
		reflect.ValueOf(current).Elem().FieldByName(name).Set(reflect.ValueOf(last))
		actual := backupEncryptionReadDelta(current, baseline)
		if actual == nil || !reflect.DeepEqual(reflect.ValueOf(actual).Elem().FieldByName(name).Interface(), first) || last[7] != 16 || first[7] != 8 {
			test.Fatalf("invalid or destructive histogram delta for %s", name)
		}
		last[7] = 0
		if backupEncryptionReadDelta(current, baseline) != nil {
			test.Fatal("histogram reset became a valid interval")
		}
		last[7] = 16
		reflect.ValueOf(baseline).Elem().FieldByName(name).SetZero()
		actual = backupEncryptionReadDelta(current, baseline)
		if actual == nil || !reflect.ValueOf(actual).Elem().FieldByName(name).IsNil() {
			test.Fatal("missing histogram baseline became available zero")
		}
	}
}

func TestBackupRPCWaitDelta(test *testing.T) {
	current := daemon.RPCWaitStats{Method: "Get", Active: 2, OldestActiveNS: 3}
	baseline := daemon.RPCWaitStats{Method: "Get", Active: 99, OldestActiveNS: 99}
	expected := current
	for _, name := range []string{"Calls", "Failures", "ElapsedWorkerNS", "ServerTimingCalls", "ServerTimingClientNS",
		"ServerHandlerNS", "ServerStorageNS", "OutsideHandlerNS", "InvalidServerTimings", "BlobReadTimingCalls",
		"BlobReadClientNS", "BlobReadHandlerNS", "BlobReadStorageNS"} {
		reflect.ValueOf(&current).Elem().FieldByName(name).SetUint(10)
		reflect.ValueOf(&baseline).Elem().FieldByName(name).SetUint(4)
		reflect.ValueOf(&expected).Elem().FieldByName(name).SetUint(6)
	}
	actual := backupRPCWaitDelta(current, baseline)
	if !reflect.DeepEqual(actual, expected) {
		test.Fatalf("RPC interval counters or active gauges differ: %+v", actual)
	}
}

type backupReconciliationStore struct {
	reconcile.Store
}

func TestBackupCloseWorkingStateCancellationPrivacyAndQuiet(t *testing.T) {
	for _, jsonOutput := range []bool{false, true} {
		t.Run(fmt.Sprintf("json=%t", jsonOutput), func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			term := &ui.MockTerminal{}
			store := archiver.NewMarkerCacheStore(1)
			run := &backupRun{ctx: ctx, markerStore: store, term: term,
				globalOptions: global.Options{JSON: jsonOutput, Quiet: true}}
			run.close()
			if !store.WorkingState().Closed {
				t.Fatal("cancellation did not close marker state")
			}
			if !jsonOutput {
				if len(term.Output) != 0 {
					t.Fatalf("quiet output: %v", term.Output)
				}
				return
			}
			if len(term.Output) != 1 {
				t.Fatalf("final events: %v", term.Output)
			}
			var record struct {
				MessageType string `json:"message_type"`
				telemetry.WorkingStateSnapshot
			}
			if err := json.Unmarshal([]byte(term.Output[0]), &record); err != nil {
				t.Fatal(err)
			}
			if record.MessageType != "working_state_stats" || record.Kind != telemetry.WorkingMarkers || record.Activated {
				t.Fatalf("unexpected lazy marker observation: %+v", record)
			}
			for _, sensitive := range []string{`"path"`, `"key"`, `"value"`, `"repository"`, `"credential"`} {
				if strings.Contains(term.Output[0], sensitive) {
					t.Fatalf("sensitive telemetry field %s", sensitive)
				}
			}
		})
	}
}

func (*backupReconciliationStore) ScanPrefix(context.Context, []byte, []byte, uint32) ([]daemon.KeyValue, bool, error) {
	return nil, true, nil
}

func TestBackupCloseJoinsReconciliationAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	reconciler, err := reconcile.New(ctx, fs.NewLocal(), &backupReconciliationStore{}, reconcile.Options{Workers: 1, QueueDepth: 1})
	if err != nil {
		t.Fatal(err)
	}
	term := &ui.MockTerminal{}
	run := &backupRun{ctx: ctx, cancel: cancel, reconciler: reconciler, term: term, globalOptions: global.Options{JSON: true}}
	run.closeSource = func() {
		if ctx.Err() == nil || len(term.Output) != 1 {
			t.Fatal("source closed before cancellation and final reconciliation stats")
		}
	}
	run.close()
	var record struct {
		MessageType string `json:"message_type"`
		reconcile.Metrics
	}
	if len(term.Output) != 1 {
		t.Fatalf("output=%v", term.Output)
	}
	if err := json.Unmarshal([]byte(term.Output[0]), &record); err != nil {
		t.Fatal(err)
	}
	if record.MessageType != "reconciliation_stats" || record.Metrics != reconciler.Metrics() ||
		record.Failed == 0 || record.FailedCanceled == 0 {
		t.Fatalf("incomplete final reconciliation stats: %+v", record)
	}
}

func TestBackupReportsReconciliationStats(t *testing.T) {
	term := &ui.MockTerminal{}
	run := &backupRun{term: term, globalOptions: global.Options{JSON: true}}
	stats := reconcile.Metrics{
		PublicationGroups: [daemon.MaxAllocatedPublicationGroupSize]uint64{1, 2, 3, 4}, RevisionAllocationCalls: 7, RevisionAllocationFailures: 1,
		RevisionAllocationFailedCanceled: 1,
		Failed:                           3, FailedCanceled: 2,
		RevisionAllocationRecoveredAborts: 3, RevisionAllocationTerminalAborts: 2,
		RevisionAllocationRecoveredRetryCalls: 2, RevisionAllocationTerminalRetryCalls: 1,
		RevisionsReserved: 20, InodeRevisionsAssigned: 18, RevisionAllocationNS: 100,
		InodePublicationCalls: 17, InodePublicationFailures: 2, InodePublicationNS: 200, PublicationGroupNS: 150,
		InodePublicationRecoveredAborts: 4, InodePublicationTerminalAborts: 1,
		InodePublicationRecoveredCommitAborts: 3, InodePublicationTerminalCommitAborts: 1,
		InodePublicationRecoveredRetryCalls: 3, InodePublicationTerminalRetryCalls: 1,
	}
	run.reportReconciliationStats(stats)
	if len(term.Output) != 1 {
		t.Fatalf("output=%v", term.Output)
	}
	var record struct {
		MessageType string `json:"message_type"`
		reconcile.Metrics
	}
	if err := json.Unmarshal([]byte(term.Output[0]), &record); err != nil {
		t.Fatal(err)
	}
	if record.MessageType != "reconciliation_stats" || record.Metrics != stats {
		t.Fatalf("invalid reconciliation stats: %+v", record)
	}
	run.globalOptions = global.Options{Quiet: true}
	run.reportReconciliationStats(stats)
	if len(term.Output) != 1 {
		t.Fatal("quiet mode emitted reconciliation stats")
	}
}

func TestBackupReportsCommitRPCStats(t *testing.T) {
	term := &ui.MockTerminal{}
	run := &backupRun{term: term, globalOptions: global.Options{JSON: true}}
	stats := daemon.CommitRPCStats{Attempts: 7, Successes: 4, Aborted: 2, OtherFailures: 1}
	run.reportCommitRPCStats(stats)
	if len(term.Output) != 1 {
		t.Fatalf("output=%v", term.Output)
	}
	var record struct {
		MessageType string `json:"message_type"`
		daemon.CommitRPCStats
	}
	if err := json.Unmarshal([]byte(term.Output[0]), &record); err != nil {
		t.Fatal(err)
	}
	if record.MessageType != "commit_rpc_stats" || record.CommitRPCStats != stats {
		t.Fatalf("invalid commit RPC stats: %+v", record)
	}
}

func TestBackupReportsCriticalPath(t *testing.T) {
	stages := backupDBStages(daemon.WriterStatus{Attribution: daemon.AttributionSnapshot{
		MultiGetDatabaseRead:    daemon.TimingSnapshot{Completed: 3, TotalUS: 7},
		MultiGetTransactionRead: daemon.TimingSnapshot{Completed: 5, TotalUS: 11},
	}})
	if stages["multi_get_database_read"].TotalUS != 7 || stages["multi_get_transaction_read"].TotalUS != 11 {
		t.Fatal("native multi-get read stages lost before interval reporting")
	}
	term := &ui.MockTerminal{}
	run := &backupRun{term: term, globalOptions: global.Options{JSON: true}}
	sample := backupCriticalPath{
		MessageType: "backup_critical_path", Phase: "main_backup", SecondsElapsed: 31,
		Reconciliation:      reconcile.Metrics{ObserveBlocked: 2, PrepareBlocked: 1, WriterPhase: "publish_batch"},
		RPC:                 []backupRPCWait{{Connection: "writer", RPCWaitStats: daemon.RPCWaitStats{Method: "Commit", Active: 4, OldestActiveNS: 100}}},
		DB:                  []backupDBWait{{Stage: "durable_wait", Active: 4, OldestActiveUS: 50}},
		DBSnapshotAvailable: true, DBIntervalAvailable: true, DBScope: "daemon_all_clients",
		BlobLookup: &enginepkg.BlobLookupStats{LookupCalls: 3, SlotWaitNanoseconds: 11, ResultWaitNanoseconds: 17, SizeRPCNanoseconds: 13, RPCConcurrencyLimit: 4},
	}
	run.reportCriticalPath(sample)
	var actual backupCriticalPath
	if len(term.Output) != 1 {
		t.Fatalf("output=%v", term.Output)
	}
	if err := json.Unmarshal([]byte(term.Output[0]), &actual); err != nil {
		t.Fatal(err)
	}
	if actual.Phase != "main_backup" || actual.Reconciliation != sample.Reconciliation || len(actual.RPC) != 1 || actual.RPC[0] != sample.RPC[0] || len(actual.DB) != 1 || actual.DB[0] != sample.DB[0] || !actual.DBIntervalAvailable {
		t.Fatalf("blocking chain not preserved: %+v", actual)
	}
	if actual.BlobLookup == nil || *actual.BlobLookup != *sample.BlobLookup {
		t.Fatalf("lookup slot/result/RPC attribution not preserved: %+v", actual.BlobLookup)
	}
	run.globalOptions = global.Options{Quiet: true}
	run.reportCriticalPath(sample)
	if len(term.Output) != 1 {
		t.Fatal("quiet mode emitted critical-path diagnostics")
	}
	if backupCounterDelta(120, 100) != 20 || backupCounterDelta(90, 100) != 0 {
		t.Fatal("startup counters or reset leaked into main-phase deltas")
	}
}

func TestBackupReportsPackPublicationStats(t *testing.T) {
	term := &ui.MockTerminal{}
	run := &backupRun{term: term, globalOptions: global.Options{JSON: true}}
	stats := daemon.PackPublicationStats{Calls: 4, Failures: 1, FailedCanceled: 1,
		RecoveredAborts: 3, RecoveredCommitAborts: 2, TerminalAborts: 1, TerminalCommitAborts: 1,
		RecoveredRetryCalls: 2, TerminalRetryCalls: 1, NS: 100}
	run.reportPackPublicationStats(stats)
	if len(term.Output) != 1 {
		t.Fatalf("output=%v", term.Output)
	}
	var record struct {
		MessageType string `json:"message_type"`
		daemon.PackPublicationStats
	}
	if err := json.Unmarshal([]byte(term.Output[0]), &record); err != nil {
		t.Fatal(err)
	}
	if record.MessageType != "pack_publication_stats" || record.PackPublicationStats != stats {
		t.Fatalf("invalid pack publication stats: %+v", record)
	}
	run.globalOptions = global.Options{Quiet: true}
	run.reportPackPublicationStats(stats)
	if len(term.Output) != 1 {
		t.Fatal("quiet mode emitted pack publication stats")
	}
}

func TestBoundedInodeManifest(t *testing.T) {
	root := t.TempDir()
	filename := filepath.Join(t.TempDir(), "manifest.json")
	manifest := BoundedInodeManifest{Version: 1, Source: root, InodeLimit: 3}
	for _, paths := range [][]string{{".", "dir", "dir/file"}, {".", "../escape"}, {".", "dir/file"}, {".", "."}, {".", "a", "b", "c"}} {
		manifest.Paths = paths
		encoded, err := json.Marshal(manifest)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filename, encoded, 0600); err != nil {
			t.Fatal(err)
		}
		selected, err := loadBoundedInodeManifest(filename, root)
		if len(paths) == 3 && paths[1] == "dir" {
			if err != nil || len(selected) != 3 || !selected[filepath.Join(root, "dir/file")] || selected[filepath.Join(root, "unselected")] {
				t.Fatalf("valid bounded selection failed: %v %v", selected, err)
			}
		} else if err == nil {
			t.Fatalf("invalid bounded selection accepted: %v", paths)
		}
	}
	manifest.Paths = []string{".", "dir", "dir/file"}
	for _, limit := range []uint64{0, 200_000, 200_001, 400_000, 800_000, 800_001} {
		manifest.InodeLimit = limit
		encoded, err := json.Marshal(manifest)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filename, encoded, 0600); err != nil {
			t.Fatal(err)
		}
		_, err = loadBoundedInodeManifest(filename, root)
		if (err == nil) != (limit > 0 && limit <= 800_000) {
			t.Fatalf("bounded selection limit %d: %v", limit, err)
		}
	}
	manifest.InodeLimit = 800_000
	manifest.Paths = make([]string, manifest.InodeLimit)
	manifest.Paths[0] = "."
	for index := 1; index < len(manifest.Paths); index++ {
		manifest.Paths[index] = fmt.Sprintf("file-%06d", index)
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filename, encoded, 0600); err != nil {
		t.Fatal(err)
	}
	selected, err := loadBoundedInodeManifest(filename, root)
	if err != nil || len(selected) != 800_000 {
		t.Fatalf("exact 800k bounded selection failed: entries=%d error=%v", len(selected), err)
	}
	if err := configureSnapshotOptions(&backupRun{options: backupOptions{BoundedInodeManifest: filename}}); err != nil {
		t.Fatal(err)
	}
	run := &backupRun{options: backupOptions{BoundedInodeManifest: filename}}
	if err := configureSnapshotOptions(run); err != nil || run.snapshotOpts.CrawlPlan.Mode != "bounded" {
		t.Fatalf("bounded source marked as a full baseline: %v", err)
	}
}

func TestBackupCloseReportsPackStatsAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	term := &ui.MockTerminal{}
	engine := enginepkg.NewDaemonEngine(&daemon.Client{})
	repo := repository.TestRepository(t)
	repo.SetEngine(&backupCloseEngine{LegacyEngine: enginepkg.NewLegacyEngine(), close: func() error {
		if len(term.Output) != 0 {
			t.Fatal("pack stats emitted before engine close")
		}
		if err := engine.SchemaStore().PublishPack(ctx, daemon.PublishedPack{}); err == nil {
			t.Fatal("invalid publication unexpectedly succeeded")
		}
		return nil
	}})
	run := &backupRun{ctx: ctx, repo: repo, authoritativeEngine: engine, term: term,
		globalOptions: global.Options{JSON: true}}
	run.close()
	var records []struct {
		MessageType string `json:"message_type"`
		daemon.PackPublicationStats
	}
	for _, output := range term.Output {
		var record struct {
			MessageType string `json:"message_type"`
			daemon.PackPublicationStats
		}
		if err := json.Unmarshal([]byte(output), &record); err == nil && record.MessageType == "pack_publication_stats" {
			records = append(records, record)
		}
	}
	if len(records) != 1 || records[0].PackPublicationStats != engine.SchemaStore().PackPublicationStats() ||
		records[0].Calls != 1 || records[0].Failures != 1 || records[0].Active != 0 {
		t.Fatalf("missing final pack outcomes: %+v output=%v", records, term.Output)
	}
}

func TestBackupCloseReportsNFSStatsAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	term := &ui.MockTerminal{}
	run := &backupRun{ctx: ctx, cancel: cancel, term: term, globalOptions: global.Options{JSON: true}}
	stats := fs.NFSStats{Lookups: 1, Getattrs: 2, ReadDirPlus: 3, Reads: 4, ReadBytes: 5, CacheHits: 6,
		ParentHits: 7, RetainedOpens: 8,
		Operations: map[string]fs.NFSOperationStats{"read": {
			Attempts: 4, Calls: 3, Errors: 1, Cancellations: 1, ErrorClasses: map[string]uint64{"cancelled": 1},
			MaxActive: 2, QueueNanoseconds: 10, ServiceNanoseconds: 20}}}
	run.closeSource = func() {
		if ctx.Err() == nil {
			t.Fatal("source stats emitted before cancellation")
		}
		run.reportNFSStats(stats)
	}
	run.close()
	if len(term.Output) != 1 {
		t.Fatalf("output=%v", term.Output)
	}
	var record struct {
		MessageType string `json:"message_type"`
		fs.NFSStats
	}
	if err := json.Unmarshal([]byte(term.Output[0]), &record); err != nil {
		t.Fatal(err)
	}
	if record.MessageType != "nfs_source_stats" || !reflect.DeepEqual(record.NFSStats, stats) {
		t.Fatalf("invalid NFS stats: %+v", record)
	}
	run.globalOptions = global.Options{Quiet: true}
	run.reportNFSStats(stats)
	if len(term.Output) != 1 {
		t.Fatal("quiet mode emitted NFS stats")
	}
}

func TestMetadataCacheBudgetFlag(t *testing.T) {
	command := NewCommand(&global.Options{})
	if limit, err := command.Flags().GetInt("metadata-read-concurrency"); err != nil || limit != 4 {
		t.Fatalf("default concurrency=%d err=%v", limit, err)
	}
	for _, options := range []backupOptions{
		{UnchangedReadConcurrency: 129}, {MetadataReadConcurrency: -1}, {MetadataReadConcurrency: 129},
		{MetadataBatchDelay: -time.Millisecond}, {MetadataBatchDelay: 6 * time.Millisecond}, {MetadataBatchDelay: time.Millisecond},
	} {
		if err := options.validateParent(); err == nil {
			t.Fatalf("invalid metadata tuning accepted: %+v", options)
		}
	}
	if budget, err := command.Flags().GetInt("metadata-cache-mib"); err != nil || budget != 64 {
		t.Fatalf("default budget=%d err=%v", budget, err)
	}
	for _, budget := range []int{0, -1, (int(^uint(0)>>1) >> 20) + 1} {
		if err := command.Flags().Set("metadata-cache-mib", strconv.Itoa(budget)); err != nil {
			t.Fatal(err)
		}
		options := backupOptions{MetadataOnDemand: true, MetadataScratch: "scratch", MetadataCacheMiB: budget}
		if err := options.validateParent(); err == nil {
			t.Fatalf("invalid budget %d accepted", budget)
		}
	}
	if err := (backupOptions{MetadataOnDemand: true, MetadataScratch: "scratch", MetadataCacheMiB: 1}).validateParent(); err != nil {
		t.Fatal(err)
	}
}

func TestBackupCloseReleasesEngineAfterUnlock(t *testing.T) {
	repo := repository.TestRepository(t)
	unlocked, closes := false, 0
	repo.SetEngine(&backupCloseEngine{LegacyEngine: enginepkg.NewLegacyEngine(), close: func() error {
		if !unlocked {
			t.Error("engine closed before repository unlock")
		}
		closes++
		return nil
	}})
	run := &backupRun{repo: repo, closeRepo: func() { unlocked = true }}
	run.close()
	if err := repo.Close(); err != nil {
		t.Fatal(err)
	}
	if closes != 1 {
		t.Fatalf("engine close calls=%d", closes)
	}
}

func TestAutomaticDeferredFallbackDistinguishesUnavailableFromCorrupt(t *testing.T) {
	options := backupOptions{AllowDeferredCommit: true, DeferredMode: "auto"}
	if !shouldUseDataPlaneFallback(fmt.Errorf("connect: %w", enginepkg.ErrUnavailable), options) {
		t.Fatal("operational metadata unavailability did not select deferred fallback")
	}
	if shouldUseDataPlaneFallback(fmt.Errorf("corrupt metadata manifest"), options) {
		t.Fatal("metadata corruption bypassed explicit acknowledgement")
	}
	options.AcknowledgeMetadataBypass = true
	if !shouldUseDataPlaneFallback(fmt.Errorf("corrupt metadata manifest"), options) {
		t.Fatal("acknowledged metadata corruption did not select deferred fallback")
	}
}

func TestMarkerPrefetchSharesSelectionCache(t *testing.T) {
	for _, caches := range []bool{false, true} {
		t.Run(fmt.Sprint(caches), func(t *testing.T) {
			root := t.TempDir()
			marker, content := ".nobackup", ""
			options := backupOptions{ExcludeIfPresent: []string{marker}}
			if caches {
				marker, content = "CACHEDIR.TAG", "Signature: 8a477f597d28d172789f06886806bc55"
				options = backupOptions{ExcludeCaches: true}
			}
			markerPath := filepath.Join(root, marker)
			if err := os.WriteFile(markerPath, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
			filesystem := fs.NewLocal()
			rejects, prefetch, err := collectRejectFuncsWithPrefetch(options, []string{root}, filesystem, t.Logf)
			if err != nil {
				t.Fatal(err)
			}
			if len(prefetch) != 1 {
				t.Fatalf("expected one marker prefetch, got %d", len(prefetch))
			}
			name := filepath.Join(root, "child")
			if archiver.CombineRejects(prefetch)(name, nil, filesystem) {
				t.Fatal("prefetch did not observe marker")
			}
			if err := os.Remove(markerPath); err != nil {
				t.Fatal(err)
			}
			selectItem := archiver.CombineRejects(rejects)
			if selectItem(name, nil, filesystem) {
				t.Fatal("selection did not reuse prefetched marker result")
			}
			if !selectItem(markerPath, nil, filesystem) {
				t.Fatal("marker itself was excluded")
			}
		})
	}
}

func TestBackupCrawlFlags(t *testing.T) {
	command := NewCommand(&global.Options{})
	for _, name := range []string{
		"use-cwalk", "no-cwalk", "cwalk-concurrency", "use-pathdiff", "pathdiff-endpoint",
		"pathdiff-require-coverage", "pathdiff-svm-map", "use-fsevents", "fsevents-require-coverage",
		"fsevents-replay-timeout", "fsevents-full-crawl-every", "apfs-snapshot", "apfs-snapshot-require", "apfs-snapshot-keep",
	} {
		if command.Flags().Lookup(name) == nil {
			t.Errorf("backup flag --%s is not registered", name)
		}
	}
	if value, err := command.Flags().GetBool("use-cwalk"); err != nil || !value {
		t.Errorf("use-cwalk = %t, %v; want true", value, err)
	}
	if value, err := command.Flags().GetInt("cwalk-concurrency"); err != nil || value != 32 {
		t.Errorf("cwalk concurrency = %d, %v; want 32", value, err)
	}
	if err := command.Flags().Set("cwalk-concurrency", "7"); err != nil {
		t.Fatal(err)
	}
	if value, err := command.Flags().GetInt("cwalk-concurrency"); err != nil || value != 7 {
		t.Errorf("configured cwalk concurrency = %d, %v; want 7", value, err)
	}
}

func TestDirectNFSSourceValidation(t *testing.T) {
	command := NewCommand(&global.Options{})
	if value, err := command.Flags().GetBool("nfs-allow-missing-metadata"); err != nil || !value {
		t.Fatalf("missing metadata default = %t, %v; want true", value, err)
	}
	if err := command.Flags().Set("nfs-allow-missing-metadata", "false"); err != nil {
		t.Fatal(err)
	}
	if value, err := command.Flags().GetBool("nfs-allow-missing-metadata"); err != nil || value {
		t.Fatalf("strict metadata option = %t, %v; want false", value, err)
	}
	targets := []string{"nfs://nas:/export/source"}
	if err := validateNFSSources(backupOptions{}, targets); err == nil {
		t.Fatal("direct NFS silently accepted missing ACL/xattr metadata")
	}
	options := backupOptions{NFSAllowMissingMetadata: true, NFSConnections: 4}
	if err := validateNFSSources(options, targets); err != nil {
		t.Fatal(err)
	}
	if filtered, err := filterExisting(targets, func(string, ...any) {}); err != nil || len(filtered) != 1 || filtered[0] != targets[0] {
		t.Fatalf("URL was treated as a local path: %v %v", filtered, err)
	}
	options.UseFsSnapshot = true
	if err := validateNFSSources(options, targets); err == nil {
		t.Fatal("accepted local snapshot with direct NFS")
	}
	options.UseFsSnapshot = false
	options.NFSConnections = 17
	if err := validateNFSSources(options, targets); err == nil {
		t.Fatal("accepted unbounded NFS connections")
	}
	options.NFSConnections = 4
	options.NFSDirect = true
	if err := validateNFSSources(options, []string{"/mnt/nfs/source"}); err != nil {
		t.Fatal(err)
	}
}

func TestNoCWalkRestoresLegacyTraversal(t *testing.T) {
	command := NewCommand(&global.Options{})
	if err := command.Flags().Set("no-cwalk", "true"); err != nil {
		t.Fatal(err)
	}
	if err := command.PreRunE(command, nil); err != nil {
		t.Fatal(err)
	}
	if value, err := command.Flags().GetBool("use-cwalk"); err != nil || value {
		t.Errorf("use-cwalk = %t, %v; want false after --no-cwalk", value, err)
	}
}

func TestBackupCrawlOptionValidation(t *testing.T) {
	globalOptions := global.Options{InsecureNoPassword: true}
	tests := []struct {
		name    string
		options backupOptions
		want    string
	}{
		{"invalid-workers", backupOptions{UseCWalk: true}, "--cwalk-concurrency must be at least 1"},
		{"on-demand-needs-scratch", backupOptions{MetadataOnDemand: true}, "--metadata-on-demand requires --metadata-scratch"},
		{"scratch-needs-on-demand", backupOptions{MetadataScratch: "scratch"}, "--metadata-scratch requires --metadata-on-demand"},
		{"cache-needs-on-demand", backupOptions{MetadataCacheMiB: 128}, "--metadata-cache-mib requires --metadata-on-demand"},
		{"cache-needs-positive-budget", backupOptions{MetadataOnDemand: true, MetadataScratch: "scratch"}, "--metadata-cache-mib must be positive"},
		{"on-demand-rejects-dry-run", backupOptions{MetadataOnDemand: true, MetadataScratch: "scratch", DryRun: true}, "--metadata-on-demand cannot use"},
		{"pathdiff-needs-cwalk", backupOptions{UsePathdiff: true}, "--use-pathdiff requires --use-cwalk"},
		{"pathdiff-needs-endpoint", backupOptions{UseCWalk: true, CWalkConcurrency: 1, UsePathdiff: true}, "--use-pathdiff requires --pathdiff-endpoint"},
		{
			"pathdiff-needs-map",
			backupOptions{UseCWalk: true, CWalkConcurrency: 1, UsePathdiff: true, PathdiffEndpoint: "socket"},
			"--use-pathdiff requires --pathdiff-svm-map",
		},
		{"coverage-needs-pathdiff", backupOptions{PathdiffRequireCoverage: true}, "--pathdiff-require-coverage requires --use-pathdiff"},
		{
			"change-sources-exclusive",
			backupOptions{
				UseCWalk: true, CWalkConcurrency: 1,
				UsePathdiff: true, PathdiffEndpoint: "socket", PathdiffSVMMap: "map",
				UseFSEvents: true, FSEventsReplayTimeout: time.Minute,
			},
			"mutually exclusive",
		},
		{"fsevents-needs-cwalk", backupOptions{UseFSEvents: true, FSEventsReplayTimeout: time.Minute}, "--use-fsevents requires --use-cwalk"},
		{"fsevents-coverage-needs-source", backupOptions{FSEventsRequireCoverage: true}, "--fsevents-require-coverage requires --use-fsevents"},
		{"fsevents-timeout-positive", backupOptions{UseCWalk: true, CWalkConcurrency: 1, UseFSEvents: true}, "--fsevents-replay-timeout must be positive"},
		{"full-crawl-duration-nonnegative", backupOptions{FSEventsFullCrawlEvery: -time.Second}, "--fsevents-full-crawl-every cannot be negative"},
		{"snapshot-require-needs-snapshot", backupOptions{APFSSnapshotRequire: true}, "--apfs-snapshot-require requires --apfs-snapshot"},
		{"snapshot-keep-needs-snapshot", backupOptions{APFSSnapshotKeep: true}, "--apfs-snapshot-keep requires --apfs-snapshot"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.options.Check(globalOptions, nil)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Check() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestBackupValidateDeferred(t *testing.T) {
	tests := []struct {
		name    string
		options backupOptions
		want    string
	}{
		{"mode-requires-opt-in", backupOptions{DeferredMode: "auto"}, "--deferred-mode requires"},
		{"mode-is-enumerated", backupOptions{AllowDeferredCommit: true, DeferredMode: "invalid"}, "requires --deferred-mode=auto"},
		{"bypass-is-acknowledged", backupOptions{AllowDeferredCommit: true, DeferredMode: "data-plane-only"}, "requires --acknowledge-metadata-bypass"},
		{"incompatible-parent", backupOptions{AllowDeferredCommit: true, DeferredMode: "auto", Parent: "latest"}, "deferred ingest cannot use"},
		{"positive-expiry", backupOptions{AllowDeferredCommit: true, DeferredMode: "auto"}, "--deferred-expiry must be positive"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.options.validateDeferred()
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validateDeferred() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestBackupValidateStdin(t *testing.T) {
	tests := []struct {
		name          string
		options       backupOptions
		globalOptions global.Options
		args          []string
		want          string
	}{
		{"password-and-data", backupOptions{Stdin: true}, global.Options{}, nil, "cannot read both password and data"},
		{"password-and-files-from", backupOptions{FilesFrom: []string{"-"}}, global.Options{}, nil, "unable to read password from stdin"},
		{
			"stdin-and-files-from", backupOptions{Stdin: true, FilesFrom: []string{"list"}},
			global.Options{InsecureNoPassword: true}, nil, "--stdin and --files-from cannot",
		},
		{"stdin-and-arguments", backupOptions{Stdin: true}, global.Options{InsecureNoPassword: true}, []string{"source"}, "files/dirs were listed"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.options.validateStdin(test.globalOptions, test.args)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validateStdin() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestBackupValidateParent(t *testing.T) {
	options := backupOptions{UseCWalk: true, CWalkConcurrency: 1, UsePathdiff: true, PathdiffEndpoint: "socket", PathdiffSVMMap: "map"}
	if err := options.validateParent(); err != nil {
		t.Fatalf("validateParent() error = %v", err)
	}
	options.PathdiffRequireCoverage = true
	if err := options.validateParent(); err != nil {
		t.Fatalf("validateParent() with coverage error = %v", err)
	}
}

func TestBackupHooksWireCallbacks(t *testing.T) {
	var reused, failed, reconciled, before, uploaded bool
	var completed, started, blobs, excluded int
	hooks := backupHooks{
		reuseSubtree:   func(string, string, *data.Node) bool { reused = true; return true },
		errorHandler:   func(string, error) error { failed = true; return nil },
		beforeSnapshot: func() error { before = true; return nil },
		reconcileNode:  func(string, string, *data.Node) { reconciled = true },
		deferredUploader: func(_ context.Context, _ func(context.Context, vaultic.BlobSaverWithAsync) error) error {
			uploaded = true
			return nil
		},
		progress: backupProgressHooks{
			completeItem: func(string, archiver.ItemAction, archiver.ItemStats, time.Duration) { completed++ },
			startFile:    func(string) { started++ },
			completeBlob: func(uint64) { blobs++ },
			excludedItem: func(string) { excluded++ },
		},
	}
	target := &archiver.Archiver{}
	hooks.wireReuseSubtree(target)
	hooks.wireError(target)
	hooks.wireProgress(target)
	hooks.wireReconciliation(target)
	var snapshotOpts archiver.SnapshotOptions
	hooks.wireDeferredUploader(&snapshotOpts)

	if !target.ReuseSubtree("", "", nil) || target.Error("", errors.New("read")) != nil {
		t.Fatal("wired reuse or error callback returned an unexpected result")
	}
	target.CompleteItem("", archiver.ItemAction(""), archiver.ItemStats{}, 0)
	target.StartFile("")
	target.CompleteBlob(0)
	target.ExcludedItem("")
	target.ReconcileNode("", "", nil)
	if err := target.BeforeSnapshot(); err != nil {
		t.Fatal(err)
	}
	if err := snapshotOpts.DeferredUploader(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if !reused || !failed || !reconciled || !before || !uploaded || completed != 1 || started != 1 || blobs != 1 || excluded != 1 {
		t.Fatalf("callbacks not all invoked: reuse=%v error=%v reconcile=%v before=%v upload=%v progress=%d/%d/%d/%d",
			reused, failed, reconciled, before, uploaded, completed, started, blobs, excluded)
	}
}

func TestBackupHooksDeferredCaptureChainsReconciliation(t *testing.T) {
	dir := rtest.TempDir(t)
	source := filepath.Join(dir, "source")
	rtest.OK(t, os.WriteFile(source, []byte("data"), 0600))
	capture := reconcile.NewDeferredCapture(fs.NewLocal())
	called := false
	target := &archiver.Archiver{ReconcileNode: func(string, string, *data.Node) { called = true }}
	hooks := backupHooks{deferredCapture: capture}
	hooks.wireDeferredCapture(target)
	target.ReconcileNode("/source", source, &data.Node{Name: "source"})
	observations, err := capture.Close()
	rtest.OK(t, err)
	if !called || len(observations) != 1 {
		t.Fatalf("deferred capture did not chain: prior=%v observations=%d", called, len(observations))
	}
}

func TestCollectTargets(t *testing.T) {
	dir := rtest.TempDir(t)

	fooSpace := "foo "
	barStar := "bar*"              // Must sort before the others, below.
	if runtime.GOOS == "windows" { // Doesn't allow "*" or trailing space.
		fooSpace = "foo"
		barStar = "bar"
	}

	var expect []string
	for _, filename := range []string{
		barStar, "baz", "cmdline arg", fooSpace,
		"fromfile", "fromfile-raw", "fromfile-verbatim", "quux",
	} {
		// All mentioned files must exist for collectTargets.
		f, err := os.Create(filepath.Join(dir, filename))
		rtest.OK(t, err)
		rtest.OK(t, f.Close())

		expect = append(expect, f.Name())
	}

	f1, err := os.Create(filepath.Join(dir, "fromfile"))
	rtest.OK(t, err)
	// Empty lines should be ignored. A line starting with '#' is a comment.
	_, err = fmt.Fprintf(f1, "\n%s*\n # here's a comment\n", f1.Name())
	rtest.OK(t, err)
	rtest.OK(t, f1.Close())

	f2, err := os.Create(filepath.Join(dir, "fromfile-verbatim"))
	rtest.OK(t, err)
	for _, filename := range []string{fooSpace, barStar} {
		// Empty lines should be ignored. CR+LF is allowed.
		_, err = fmt.Fprintf(f2, "%s\r\n\n", filepath.Join(dir, filename))
		rtest.OK(t, err)
	}
	rtest.OK(t, f2.Close())

	f3, err := os.Create(filepath.Join(dir, "fromfile-raw"))
	rtest.OK(t, err)
	for _, filename := range []string{"baz", "quux"} {
		_, err = fmt.Fprintf(f3, "%s\x00", filepath.Join(dir, filename))
		rtest.OK(t, err)
	}
	rtest.OK(t, err)
	rtest.OK(t, f3.Close())

	options := backupOptions{
		FilesFrom:         []string{f1.Name()},
		FilesFromVerbatim: []string{f2.Name()},
		FilesFromRaw:      []string{f3.Name()},
	}

	targets, err := collectTargets(options, []string{filepath.Join(dir, "cmdline arg")}, t.Logf, nil)
	rtest.OK(t, err)
	sort.Strings(targets)
	rtest.Equals(t, expect, targets)

	_, err = collectTargets(options, []string{filepath.Join(dir, "cmdline arg"), filepath.Join(dir, "non-existing-file")}, t.Logf, nil)
	rtest.Assert(t, errors.Is(err, ErrInvalidSourceData), "expected error when not all targets exist")
}

func TestFilterExistingUnreadable(t *testing.T) {
	dir := rtest.TempDir(t)

	existing := filepath.Join(dir, "existing")
	rtest.OK(t, os.Mkdir(existing, 0755))

	file := filepath.Join(dir, "file")
	rtest.OK(t, os.WriteFile(file, []byte("x"), 0600))

	// Regression test for #5667. A target whose Lstat fails with an error other
	// than ErrNotExist must be skipped (ENOTDIR on unix, NUL byte everywhere).
	for _, unreadable := range []string{filepath.Join(file, "child"), "invalid\x00path"} {
		result, err := filterExisting([]string{unreadable}, t.Logf)
		rtest.Assert(t, errors.Is(err, ErrNoSourceData), "input %q: expected ErrNoSourceData; got %v", unreadable, err)
		rtest.Assert(t, len(result) == 0, "input %q: expected no targets; got %v", unreadable, result)

		result, err = filterExisting([]string{existing, unreadable}, t.Logf)
		rtest.Assert(t, errors.Is(err, ErrInvalidSourceData), "input %q: expected ErrInvalidSourceData; got %v", unreadable, err)
		rtest.Equals(t, []string{existing}, result)
	}
}

func TestReadFilenamesRaw(t *testing.T) {
	// These should all be returned exactly as-is.
	expected := []string{
		"\xef\xbb\xbf/utf-8-bom",
		"/absolute",
		"../.././relative",
		"\t\t leading and trailing space   \t\t",
		"newline\nin filename",
		"not UTF-8: \x80\xff/simple",
		` / *[]* \ `,
	}

	var buf bytes.Buffer
	for _, name := range expected {
		buf.WriteString(name)
		buf.WriteByte(0)
	}

	got, err := readFilenamesRaw(&buf)
	rtest.OK(t, err)
	rtest.Equals(t, expected, got)

	// Empty input is ok.
	got, err = readFilenamesRaw(strings.NewReader(""))
	rtest.OK(t, err)
	rtest.Equals(t, 0, len(got))

	// An empty filename is an error.
	_, err = readFilenamesRaw(strings.NewReader("foo\x00\x00"))
	rtest.Assert(t, err != nil, "no error for zero byte")
	rtest.Assert(t, strings.Contains(err.Error(), "empty filename"),
		"wrong error message: %v", err.Error())

	// No trailing NUL byte is an error, because it likely means we're
	// reading a line-oriented text file (someone forgot -print0).
	_, err = readFilenamesRaw(strings.NewReader("simple.txt"))
	rtest.Assert(t, err != nil, "no error for zero byte")
	rtest.Assert(t, strings.Contains(err.Error(), "zero byte"),
		"wrong error message: %v", err.Error())
}
