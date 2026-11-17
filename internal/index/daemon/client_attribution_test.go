package daemon

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	vaulticdbv1 "github.com/otuschhoff/vaultic/internal/index/proto/vaulticdb/v1"
)

func TestEncryptionReadMetricsCompatibility(test *testing.T) {
	for _, encoded := range []string{"", "null", "[]", "{", "{}", `{"reads":-1}`, `{"reads":1}`, strings.Repeat(" ", 4097)} {
		if metrics := encryptionReadMetrics([]byte(encoded)); metrics != nil {
			test.Fatalf("invalid optional telemetry accepted: %q", encoded)
		}
	}
	encoded, err := json.Marshal(EncryptionReadMetrics{Reads: 2, RequestedPlaintextBytes: 7, RoundedCiphertextBytes: 32})
	if err != nil {
		test.Fatal(err)
	}
	if metrics := encryptionReadMetrics([]byte(strings.Replace(string(encoded), `"reads":2`, `"reads":null`, 1))); metrics != nil {
		test.Fatal("null counter became an available zero")
	}
	status := writerStatus(&vaulticdbv1.WriterStatusResponse{Attribution: &vaulticdbv1.AttributionSnapshot{EncryptionReadMetricsJson: encoded}})
	if status.Attribution.EncryptionReads == nil || status.Attribution.EncryptionReads.Reads != 2 || status.Attribution.EncryptionReads.RequestedPlaintextBytes != 7 || status.Attribution.EncryptionReads.RoundedCiphertextBytes != 32 {
		test.Fatalf("optional encryption metrics lost: %+v", status.Attribution.EncryptionReads)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		test.Fatal(err)
	}
	for _, name := range []string{"header_validation_calls", "header_validation_ns", "header_validation_coalesced_reads", "chunk_cache_hits", "chunk_cache_misses", "chunk_cache_evictions", "chunk_coalesced_reads"} {
		delete(fields, name)
	}
	optional := []string{"chunk_sampled_first_fetches", "chunk_sampled_refetches", "chunk_sampled_history_evictions", "chunk_sampled_reuse_ns", "chunk_sampled_reuse_bytes", "chunk_sampled_refetch_within_64m", "chunk_sampled_refetch_within_256m", "chunk_sampled_refetch_within_1g", "read_windows_dropped"}
	for _, name := range optional {
		delete(fields, name)
	}
	older, err := json.Marshal(fields)
	if err != nil || encryptionReadMetrics(older) == nil {
		test.Fatal("older daemon without chunk counters became unavailable")
	}
	for _, name := range append(optional, "header_validation_calls", "header_validation_ns", "header_validation_coalesced_reads", "chunk_cache_hits", "chunk_cache_misses", "chunk_cache_evictions", "chunk_coalesced_reads") {
		for _, invalid := range []string{"null", "-1", "18446744073709551616", `"invalid"`} {
			fields[name] = json.RawMessage(invalid)
			malformed, err := json.Marshal(fields)
			if err != nil || encryptionReadMetrics(malformed) != nil {
				test.Fatalf("invalid optional chunk metric accepted: %s=%s", name, invalid)
			}
		}
		fields[name] = json.RawMessage("7")
	}
	extended, err := json.Marshal(fields)
	if err != nil {
		test.Fatal(err)
	}
	status = writerStatus(&vaulticdbv1.WriterStatusResponse{Attribution: &vaulticdbv1.AttributionSnapshot{EncryptionReadMetricsJson: extended}})
	if status.Attribution.EncryptionReads == nil || status.Attribution.EncryptionReads.ChunkCacheHits != 7 || status.Attribution.EncryptionReads.ChunkCoalescedReads != 7 || status.Attribution.EncryptionReads.HeaderValidationNanoseconds != 7 || status.Attribution.EncryptionReads.HeaderValidationCoalescedReads != 7 {
		test.Fatalf("chunk metrics lost in writer status: %+v", status.Attribution.EncryptionReads)
	}
}

func TestEncryptionOriginStagesCompatibility(test *testing.T) {
	encoded, err := json.Marshal(EncryptionReadMetrics{
		CiphertextOriginRequestNS: 13, CiphertextOriginBodyNS: 29, CiphertextOriginBodyCalls: 2,
	})
	if err != nil {
		test.Fatal(err)
	}
	status := writerStatus(&vaulticdbv1.WriterStatusResponse{Attribution: &vaulticdbv1.AttributionSnapshot{EncryptionReadMetricsJson: encoded}})
	metrics := status.Attribution.EncryptionReads
	if metrics == nil || metrics.CiphertextOriginRequestNS != 13 || metrics.CiphertextOriginBodyNS != 29 || metrics.CiphertextOriginBodyCalls != 2 {
		test.Fatalf("origin stages lost in status: %+v", metrics)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		test.Fatal(err)
	}
	names := []string{"ciphertext_origin_request_ns", "ciphertext_origin_body_ns", "ciphertext_origin_body_calls"}
	for name := range fields {
		if strings.HasPrefix(name, "ciphertext_origin_request_") || strings.HasPrefix(name, "ciphertext_origin_body_") || name == "ciphertext_origin_identity_rejections" {
			delete(fields, name)
		}
	}
	for _, name := range names {
		delete(fields, name)
	}
	legacy, err := json.Marshal(fields)
	if err != nil || encryptionReadMetrics(legacy) == nil {
		test.Fatal("legacy origin metrics became unavailable")
	}
	for _, name := range names {
		for _, invalid := range []string{"null", "-1", `"invalid"`, "18446744073709551616"} {
			fields[name] = json.RawMessage(invalid)
			malformed, err := json.Marshal(fields)
			if err != nil || encryptionReadMetrics(malformed) != nil {
				test.Fatalf("invalid origin stage accepted: %s=%s", name, invalid)
			}
		}
		fields[name] = json.RawMessage("7")
		partial, err := json.Marshal(fields)
		if err != nil || encryptionReadMetrics(partial) != nil {
			test.Fatalf("partial origin stage set accepted: %s", name)
		}
		delete(fields, name)
	}
}

func TestEncryptionOriginOutcomesCompatibility(test *testing.T) {
	expected := EncryptionReadMetrics{
		CiphertextOriginRequestNS: 13, CiphertextOriginBodyNS: 29, CiphertextOriginBodyCalls: 2,
		CiphertextOriginRequestSuccesses: 3, CiphertextOriginRequestNotFound: 5,
		CiphertextOriginRequestErrors: 7, CiphertextOriginRequestCancellations: 11,
		CiphertextOriginRequestActive: 17, CiphertextOriginIdentityRejections: 19,
		CiphertextOriginBodySuccesses: 23, CiphertextOriginBodyErrors: 31,
		CiphertextOriginBodyCancellations: 37, CiphertextOriginBodyActive: 41,
	}
	encoded, err := json.Marshal(expected)
	if err != nil {
		test.Fatal(err)
	}
	status := writerStatus(&vaulticdbv1.WriterStatusResponse{Attribution: &vaulticdbv1.AttributionSnapshot{EncryptionReadMetricsJson: encoded}})
	if !reflect.DeepEqual(status.Attribution.EncryptionReads, &expected) {
		test.Fatalf("origin outcomes lost in status: %+v", status.Attribution.EncryptionReads)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		test.Fatal(err)
	}
	names := []string{"ciphertext_origin_request_successes", "ciphertext_origin_request_not_found",
		"ciphertext_origin_request_errors", "ciphertext_origin_request_cancellations", "ciphertext_origin_request_active",
		"ciphertext_origin_identity_rejections", "ciphertext_origin_body_successes", "ciphertext_origin_body_errors",
		"ciphertext_origin_body_cancellations", "ciphertext_origin_body_active"}
	for _, name := range names {
		original := fields[name]
		for _, invalid := range []string{"null", "-1", `"invalid"`, "true", "18446744073709551616"} {
			fields[name] = json.RawMessage(invalid)
			malformed, err := json.Marshal(fields)
			if err != nil || encryptionReadMetrics(malformed) != nil {
				test.Fatalf("invalid origin outcome accepted: %s=%s", name, invalid)
			}
		}
		delete(fields, name)
		partial, err := json.Marshal(fields)
		if err != nil || encryptionReadMetrics(partial) != nil {
			test.Fatalf("partial outcomes accepted without %s", name)
		}
		fields[name] = original
	}
	delete(fields, "ciphertext_origin_request_ns")
	missingPhase, _ := json.Marshal(fields)
	if encryptionReadMetrics(missingPhase) != nil {
		test.Fatal("outcomes without phase timers accepted")
	}
	fields["ciphertext_origin_request_ns"] = json.RawMessage("13")
	for _, name := range names {
		delete(fields, name)
	}
	legacy, err := json.Marshal(fields)
	if err != nil || encryptionReadMetrics(legacy) == nil {
		test.Fatal("legacy phase-only daemon rejected")
	}
}

func TestEncryptionOriginErrorDetailsCompatibility(test *testing.T) {
	expected := EncryptionReadMetrics{CiphertextOriginRequestErrorCounts: map[string]uint64{
		"dropped": 0, "header/precondition/conditional/io_unclassified": 7,
		"range/generic/plain/io_permission_denied": 3,
	}}
	encoded, err := json.Marshal(expected)
	if err != nil {
		test.Fatal(err)
	}
	status := writerStatus(&vaulticdbv1.WriterStatusResponse{Attribution: &vaulticdbv1.AttributionSnapshot{EncryptionReadMetricsJson: encoded}})
	if !reflect.DeepEqual(status.Attribution.EncryptionReads, &expected) {
		test.Fatalf("origin error details lost in status: %+v", status.Attribution.EncryptionReads)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		test.Fatal(err)
	}
	for _, invalid := range []string{
		"null", "{}", "[]", `{"header/precondition/conditional/io_unclassified":1}`,
		`{"dropped":null}`, `{"dropped":-1}`, `{"dropped":true}`, `{"dropped":18446744073709551616}`,
		`{"dropped":0,"private/path":1}`, `{"dropped":0,"header/unknown/plain/io_other":1}`,
		`{"dropped":0,"header/generic/plain/io_other":null}`, `{"dropped":0,"header/generic/plain/io_other":"1"}`,
	} {
		fields["ciphertext_origin_request_error_counts"] = json.RawMessage(invalid)
		payload, err := json.Marshal(fields)
		if err != nil || encryptionReadMetrics(payload) != nil {
			test.Fatalf("malformed origin error details accepted: %s", invalid)
		}
	}
	oversized := map[string]uint64{"dropped": 0}
	for _, site := range []string{"header", "chunk", "range"} {
		for _, kind := range []string{"generic", "not_found", "precondition"} {
			for _, condition := range []string{"plain", "conditional"} {
				for _, cause := range []string{"io_unclassified", "io_not_found", "io_permission_denied", "io_invalid_input", "io_unexpected_eof", "io_timed_out", "io_interrupted", "io_would_block", "io_other"} {
					oversized[strings.Join([]string{site, kind, condition, cause}, "/")] = 1
				}
			}
		}
	}
	fields["ciphertext_origin_request_error_counts"], _ = json.Marshal(oversized)
	payload, _ := json.Marshal(fields)
	if encryptionReadMetrics(payload) != nil {
		test.Fatal("unbounded origin error details accepted")
	}
	fields["ciphertext_origin_request_error_counts"] = json.RawMessage(`{"dropped":0}`)
	delete(fields, "ciphertext_origin_request_errors")
	payload, _ = json.Marshal(fields)
	if encryptionReadMetrics(payload) != nil {
		test.Fatal("error details without outcome counters accepted")
	}
	delete(fields, "ciphertext_origin_request_error_counts")
	fields["ciphertext_origin_request_errors"] = json.RawMessage("0")
	payload, _ = json.Marshal(fields)
	if metrics := encryptionReadMetrics(payload); metrics == nil || metrics.CiphertextOriginRequestErrorCounts != nil {
		test.Fatal("legacy missing details became available or broke decoding")
	}
}

func TestEncryptionOriginBodyPayloadsCompatibility(test *testing.T) {
	expected := EncryptionReadMetrics{CiphertextOriginBodyPayloads: &EncryptionOriginBodyPayloads{
		FileCalls: 3, FileNS: 5, StreamCalls: 7, StreamNS: 11,
	}}
	encoded, err := json.Marshal(expected)
	if err != nil {
		test.Fatal(err)
	}
	status := writerStatus(&vaulticdbv1.WriterStatusResponse{Attribution: &vaulticdbv1.AttributionSnapshot{EncryptionReadMetricsJson: encoded}})
	if !reflect.DeepEqual(status.Attribution.EncryptionReads, &expected) {
		test.Fatal("body payload attribution lost through WriterStatus")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		test.Fatal(err)
	}
	name := "ciphertext_origin_body_payloads"
	for _, invalid := range []string{"null", "{}", "[]", `{"file_calls":1,"file_ns":2,"stream_calls":3}`, `{"file_calls":1,"file_ns":2,"stream_calls":3,"stream_ns":4,"extra":5}`} {
		fields[name] = json.RawMessage(invalid)
		payload, _ := json.Marshal(fields)
		if encryptionReadMetrics(payload) != nil {
			test.Fatalf("invalid body payload group accepted: %s", invalid)
		}
	}
	for _, counter := range []string{"file_calls", "file_ns", "stream_calls", "stream_ns"} {
		for _, invalid := range []string{"null", "true", `"1"`, "-1", "1.5", "18446744073709551616"} {
			group := map[string]json.RawMessage{"file_calls": []byte("0"), "file_ns": []byte("0"), "stream_calls": []byte("0"), "stream_ns": []byte("0")}
			group[counter] = json.RawMessage(invalid)
			fields[name], _ = json.Marshal(group)
			payload, _ := json.Marshal(fields)
			if encryptionReadMetrics(payload) != nil {
				test.Fatalf("invalid body payload counter accepted: %s=%s", counter, invalid)
			}
		}
	}
	fields[name] = json.RawMessage(`{"file_calls":0,"file_ns":0,"stream_calls":0,"stream_ns":0}`)
	payload, _ := json.Marshal(fields)
	if metrics := encryptionReadMetrics(payload); metrics == nil || metrics.CiphertextOriginBodyPayloads == nil {
		test.Fatal("measured zero body payload group became unavailable")
	}
	delete(fields, name)
	payload, _ = json.Marshal(fields)
	if metrics := encryptionReadMetrics(payload); metrics == nil || metrics.CiphertextOriginBodyPayloads != nil {
		test.Fatal("legacy absence became measured body payload attribution")
	}
}

func TestEncryptionOriginFileReadCompatibility(test *testing.T) {
	expected := EncryptionReadMetrics{
		CiphertextOriginBodyPayloads: &EncryptionOriginBodyPayloads{},
		CiphertextOriginFileRead: &EncryptionOriginFileRead{
			Calls: 1, Started: 2, Finished: 3, Errors: 4, Panics: 5, Abandoned: 6, Active: 7,
			QueueNS: 8, ServiceNS: 9, Resumed: 10, CompletionNS: 11, Cancellations: 12, DirectCalls: 13,
		},
	}
	encoded, err := json.Marshal(expected)
	if err != nil {
		test.Fatal(err)
	}
	status := writerStatus(&vaulticdbv1.WriterStatusResponse{Attribution: &vaulticdbv1.AttributionSnapshot{EncryptionReadMetricsJson: encoded}})
	if !reflect.DeepEqual(status.Attribution.EncryptionReads, &expected) {
		test.Fatal("file read diagnostics lost through WriterStatus")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		test.Fatal(err)
	}
	name := "ciphertext_origin_file_read"
	var valid map[string]json.RawMessage
	if err := json.Unmarshal(fields[name], &valid); err != nil {
		test.Fatal(err)
	}
	checkInvalid := func(group json.RawMessage) {
		test.Helper()
		fields[name] = group
		payload, _ := json.Marshal(fields)
		if encryptionReadMetrics(payload) != nil {
			test.Fatalf("invalid file read group accepted: %s", group)
		}
	}
	for _, invalid := range []string{"null", "{}", "[]"} {
		checkInvalid(json.RawMessage(invalid))
	}
	for counter, original := range valid {
		delete(valid, counter)
		group, _ := json.Marshal(valid)
		checkInvalid(group)
		for _, invalid := range []string{"null", "true", `"1"`, "-1", "1.5", "18446744073709551616"} {
			valid[counter] = json.RawMessage(invalid)
			group, _ = json.Marshal(valid)
			checkInvalid(group)
		}
		valid[counter] = original
	}
	valid["extra"] = json.RawMessage("0")
	group, _ := json.Marshal(valid)
	checkInvalid(group)
	fields[name], _ = json.Marshal(EncryptionOriginFileRead{})
	payload, _ := json.Marshal(fields)
	if metrics := encryptionReadMetrics(payload); metrics == nil || metrics.CiphertextOriginFileRead == nil {
		test.Fatal("measured zero file read group became unavailable")
	}
	delete(fields, "ciphertext_origin_body_payloads")
	checkInvalid(fields[name])
	delete(fields, name)
	payload, _ = json.Marshal(fields)
	if metrics := encryptionReadMetrics(payload); metrics == nil || metrics.CiphertextOriginFileRead != nil {
		test.Fatal("legacy absence became measured file read attribution")
	}
}

func TestEncryptionOriginNotModifiedCompatibility(test *testing.T) {
	count := uint64(3)
	expected := EncryptionReadMetrics{
		CiphertextOriginRequestNotModified: &count,
		CiphertextOriginRequestErrorCounts: map[string]uint64{"dropped": 0, "header/not_modified/conditional/io_unclassified": 3},
	}
	encoded, err := json.Marshal(expected)
	if err != nil {
		test.Fatal(err)
	}
	status := writerStatus(&vaulticdbv1.WriterStatusResponse{Attribution: &vaulticdbv1.AttributionSnapshot{EncryptionReadMetricsJson: encoded}})
	if !reflect.DeepEqual(status.Attribution.EncryptionReads, &expected) {
		test.Fatalf("conditional not-modified count lost: %+v", status.Attribution.EncryptionReads)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		test.Fatal(err)
	}
	name := "ciphertext_origin_request_not_modified"
	for _, invalid := range []string{"null", "-1", `"3"`, "true", "18446744073709551616"} {
		fields[name] = json.RawMessage(invalid)
		payload, err := json.Marshal(fields)
		if err != nil || encryptionReadMetrics(payload) != nil {
			test.Fatalf("invalid conditional counter accepted: %s", invalid)
		}
	}
	fields[name] = json.RawMessage("0")
	payload, _ := json.Marshal(fields)
	if metrics := encryptionReadMetrics(payload); metrics == nil || metrics.CiphertextOriginRequestNotModified == nil || *metrics.CiphertextOriginRequestNotModified != 0 {
		test.Fatal("measured zero became unavailable")
	}
	delete(fields, name)
	payload, _ = json.Marshal(fields)
	if metrics := encryptionReadMetrics(payload); metrics == nil || metrics.CiphertextOriginRequestNotModified != nil {
		test.Fatal("legacy absence became an available zero")
	}
	fields[name] = json.RawMessage("3")
	delete(fields, "ciphertext_origin_request_error_counts")
	payload, _ = json.Marshal(fields)
	if encryptionReadMetrics(payload) != nil {
		test.Fatal("conditional count without request details accepted")
	}
}

func TestEncryptionReadLatencyCompatibility(test *testing.T) {
	encoded, err := json.Marshal(EncryptionReadMetrics{})
	if err != nil {
		test.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		test.Fatal(err)
	}
	for _, name := range []string{"ciphertext_origin_latency_buckets", "chunk_wait_latency_buckets", "chunk_admission_latency_buckets"} {
		for _, invalid := range []string{"null", "[]", "[1]", "[0,0,0,0,0,0,0,0,0]", "[0,0,0,0,0,0,0,null]", "[0,0,0,0,0,0,0,-1]", "[0,0,0,0,0,0,0,18446744073709551616]"} {
			fields[name] = json.RawMessage(invalid)
			payload, err := json.Marshal(fields)
			if err != nil || encryptionReadMetrics(payload) != nil {
				test.Fatalf("accepted malformed latency metric %s=%s", name, invalid)
			}
		}
		fields[name] = json.RawMessage("[1,2,3,4,5,6,7,8]")
	}
	payload, err := json.Marshal(fields)
	if err != nil {
		test.Fatal(err)
	}
	status := writerStatus(&vaulticdbv1.WriterStatusResponse{Attribution: &vaulticdbv1.AttributionSnapshot{EncryptionReadMetricsJson: payload}})
	metrics := status.Attribution.EncryptionReads
	if metrics == nil || metrics.CiphertextOriginLatencyBuckets == nil || *metrics.CiphertextOriginLatencyBuckets != [8]uint64{1, 2, 3, 4, 5, 6, 7, 8} || metrics.ChunkWaitLatencyBuckets == nil || metrics.ChunkAdmissionLatencyBuckets == nil {
		test.Fatal("latency histograms lost through writer status")
	}
	older := encryptionReadMetrics(encoded)
	if older == nil || older.CiphertextOriginLatencyBuckets != nil || older.ChunkWaitLatencyBuckets != nil {
		test.Fatal("older daemon latency became available zero")
	}
}

func TestEncryptionReadWindowsCompatibility(test *testing.T) {
	encoded, err := json.Marshal(EncryptionReadMetrics{})
	if err != nil {
		test.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		test.Fatal(err)
	}
	for _, invalid := range []string{"null", "[null]", "[{}]",
		`[{"unix_seconds":10,"requested_plaintext_bytes":null,"ciphertext_origin_bytes":1}]`,
		`[{"unix_seconds":11,"requested_plaintext_bytes":1,"ciphertext_origin_bytes":1}]`,
		`[{"unix_seconds":10,"requested_plaintext_bytes":1,"ciphertext_origin_bytes":1},{"unix_seconds":10,"requested_plaintext_bytes":1,"ciphertext_origin_bytes":1}]`} {
		fields["read_windows"] = json.RawMessage(invalid)
		payload, err := json.Marshal(fields)
		if err != nil || encryptionReadMetrics(payload) != nil {
			test.Fatalf("accepted invalid windows: %s (%v)", invalid, err)
		}
	}
	windows := make([]EncryptionReadWindow, 360)
	for index := range windows {
		windows[index] = EncryptionReadWindow{UnixSeconds: uint64(index * 10), RequestedPlaintextBytes: 1, CiphertextOriginBytes: 2}
	}
	encoded, err = json.Marshal(EncryptionReadMetrics{ReadWindows: windows})
	if err != nil || encryptionReadMetrics(encoded) == nil {
		test.Fatal("bounded populated windows became unavailable", err)
	}
}

func TestCacheReadMetricsCompatibility(test *testing.T) {
	valid := `{"block":{"hits":2,"misses":3},"index":{"hits":4,"misses":5},"filter":{"hits":6,"misses":7},"stats":{"hits":8,"misses":9}}`
	for _, encoded := range []string{"", "null", "[]", "{", "{}", strings.Repeat(" ", 4097), strings.Replace(valid, `"hits":2`, `"hits":null`, 1), strings.Replace(valid, `"hits":2`, `"hits":-1`, 1), strings.Replace(valid, `"hits":2`, `"hits":18446744073709551616`, 1), strings.Replace(valid, `{"hits":2,"misses":3}`, `null`, 1)} {
		if metrics := cacheReadMetrics([]byte(encoded)); metrics != nil {
			test.Fatalf("invalid optional cache telemetry accepted: %q", encoded)
		}
	}
	status := writerStatus(&vaulticdbv1.WriterStatusResponse{Attribution: &vaulticdbv1.AttributionSnapshot{CacheReadMetricsJson: []byte(valid)}})
	if status.Attribution.CacheReads == nil || (*status.Attribution.CacheReads)["block"] != (CacheLookupMetrics{Hits: 2, Misses: 3}) || (*status.Attribution.CacheReads)["stats"] != (CacheLookupMetrics{Hits: 8, Misses: 9}) {
		test.Fatalf("optional cache telemetry lost: %+v", status.Attribution.CacheReads)
	}
	older := writerStatus(&vaulticdbv1.WriterStatusResponse{Attribution: &vaulticdbv1.AttributionSnapshot{}})
	if older.Attribution.CacheReads != nil {
		test.Fatal("older daemon became an available zero")
	}
}

func TestWriterStatusAttributionMapping(t *testing.T) {
	status := writerStatus(&vaulticdbv1.WriterStatusResponse{
		ProcessStartedUnixMs: 100,
		CapturedUnixMs:       200,
		Attribution: &vaulticdbv1.AttributionSnapshot{
			AdmissionWait: &vaulticdbv1.TimingSnapshot{
				Attempts:             3,
				Failures:             1,
				TotalUs:              17,
				MaxUs:                9,
				Completed:            3,
				Successes:            1,
				Cancellations:        1,
				Timeouts:             1,
				Active:               2,
				OldestActiveUs:       23,
				LatencyBucketUpperUs: []uint64{10, 100},
				LatencyBucketCounts:  []uint64{1, 2},
				ContentionAvailable:  true,
				Contentions:          4,
				ActiveOverflow:       5,
			},
			AdmissionLockHold:                       &vaulticdbv1.TimingSnapshot{Completed: 7},
			EngineWriteBatches:                      5,
			EngineBackpressureCount:                 2,
			EngineCompactedBytes:                    1024,
			EngineMemtableWriteBytes:                2048,
			EngineWalFlushBytes:                     4096,
			EngineGetKeys:                           11,
			EngineMultiGetCalls:                     12,
			EngineMultiGetInputKeys:                 13,
			EngineMultiGetUniqueKeys:                14,
			EngineMultiGetSstVisits:                 15,
			EngineMultiGetCandidateKeys:             16,
			EngineMultiGetNeededBlocks:              17,
			EngineMultiGetCoalescedReads:            18,
			EngineMultiGetNeededBlockBytes:          19,
			EngineMultiGetCoalescedReadBytes:        20,
			EngineMultiGetProjectedReadsGap_8:       21,
			EngineMultiGetProjectedReadBytesGap_8:   22,
			EngineMultiGetProjectedReadsGap_32:      23,
			EngineMultiGetProjectedReadBytesGap_32:  24,
			EngineMultiGetProjectedReadsGap_128:     25,
			EngineMultiGetProjectedReadBytesGap_128: 26,
			EngineReadMetricsAvailable:              true,
			EngineMultiGetMetricsAvailable:          true,
			EngineBackpressure: &vaulticdbv1.TimingSnapshot{
				Attempts:             9,
				Failures:             2,
				Completed:            8,
				Successes:            5,
				Cancellations:        1,
				Timeouts:             4,
				Active:               1,
				OldestActiveUs:       6000,
				TotalUs:              7000,
				MaxUs:                5000,
				LatencyBucketUpperUs: []uint64{1000, 5000},
				LatencyBucketCounts:  []uint64{3, 5},
			},
			EngineBatchWriteQueueDepth: 3,
			EngineBatchWriteQueue: &vaulticdbv1.TimingSnapshot{
				Completed:     11,
				Successes:     8,
				Failures:      2,
				Cancellations: 1,
			},
			EngineBatchWriteService: &vaulticdbv1.TimingSnapshot{
				Completed:     10,
				Successes:     7,
				Failures:      2,
				Cancellations: 1,
				Active:        1,
			},
			ObjectStoreMain: &vaulticdbv1.ObjectStoreRoleSnapshot{
				Put: &vaulticdbv1.ObjectOperationSnapshot{
					Timing:                    &vaulticdbv1.TimingSnapshot{Attempts: 2, Successes: 1, Active: 1},
					TransferredBytes:          512,
					TransferredBytesAvailable: true,
				},
			},
			ObjectStoreWal: &vaulticdbv1.ObjectStoreRoleSnapshot{
				MultipartPart: &vaulticdbv1.ObjectOperationSnapshot{
					Timing:                    &vaulticdbv1.TimingSnapshot{Failures: 1},
					TransferredBytesAvailable: true,
				},
			},
			ObjectStoreCoordination: &vaulticdbv1.ObjectStoreRoleSnapshot{
				Head:                        &vaulticdbv1.ObjectOperationSnapshot{Timing: &vaulticdbv1.TimingSnapshot{Completed: 3}},
				RetryDelayAvailable:         false,
				BackgroundPressureAvailable: false,
			},
		},
	})
	if status.ProcessStartedUnixMS != 100 || status.CapturedUnixMS != 200 {
		t.Fatalf("status identity = %+v", status)
	}

	wait := status.Attribution.AdmissionWait
	if wait.Attempts != 3 || wait.Failures != 1 || wait.TotalUS != 17 || wait.MaxUS != 9 || wait.Completed != 3 || wait.Successes != 1 || wait.Cancellations != 1 || wait.Timeouts != 1 || wait.Active != 2 || wait.OldestActiveUS != 23 || wait.ActiveOverflow != 5 || !wait.ContentionAvailable || wait.Contentions != 4 {
		t.Fatalf("admission snapshot = %+v", status.Attribution.AdmissionWait)
	}
	if len(wait.LatencyBucketUpperUS) != 2 || wait.LatencyBucketUpperUS[1] != 100 || len(wait.LatencyBucketCounts) != 2 || wait.LatencyBucketCounts[1] != 2 {
		t.Fatalf("admission histogram = %+v", wait)
	}
	if status.Attribution.AdmissionLockHold.Completed != 7 {
		t.Fatalf("admission lock hold = %+v", status.Attribution.AdmissionLockHold)
	}
	if status.Attribution.EngineWriteBatches != 5 || status.Attribution.EngineBackpressureCount != 2 || status.Attribution.EngineCompactedBytes != 1024 || status.Attribution.EngineMemtableWriteBytes != 2048 || status.Attribution.EngineWALFlushBytes != 4096 {
		t.Fatalf("engine snapshot = %+v", status.Attribution)
	}
	if status.Attribution.EngineGetKeys != 11 || status.Attribution.EngineMultiGetCalls != 12 || status.Attribution.EngineMultiGetInputKeys != 13 || status.Attribution.EngineMultiGetUniqueKeys != 14 || status.Attribution.EngineMultiGetSSTVisits != 15 || status.Attribution.EngineMultiGetCandidateKeys != 16 || status.Attribution.EngineMultiGetNeededBlocks != 17 || status.Attribution.EngineMultiGetCoalescedReads != 18 || status.Attribution.EngineMultiGetNeededBlockBytes != 19 || status.Attribution.EngineMultiGetCoalescedReadBytes != 20 || status.Attribution.EngineMultiGetProjectedReadsGap8 != 21 || status.Attribution.EngineMultiGetProjectedReadBytesGap8 != 22 || status.Attribution.EngineMultiGetProjectedReadsGap32 != 23 || status.Attribution.EngineMultiGetProjectedReadBytesGap32 != 24 || status.Attribution.EngineMultiGetProjectedReadsGap128 != 25 || status.Attribution.EngineMultiGetProjectedReadBytesGap128 != 26 || !status.Attribution.EngineReadMetricsAvailable || !status.Attribution.EngineMultiGetMetricsAvailable {
		t.Fatalf("engine read snapshot = %+v", status.Attribution)
	}
	backpressure := status.Attribution.EngineBackpressure
	if backpressure.Attempts != 9 || backpressure.Completed != 8 || backpressure.Successes != 5 || backpressure.Failures != 2 || backpressure.Cancellations != 1 || backpressure.Timeouts != 4 || backpressure.Active != 1 || backpressure.OldestActiveUS != 6000 || backpressure.TotalUS != 7000 || backpressure.MaxUS != 5000 || !reflect.DeepEqual(backpressure.LatencyBucketUpperUS, []uint64{1000, 5000}) || !reflect.DeepEqual(backpressure.LatencyBucketCounts, []uint64{3, 5}) {
		t.Fatalf("engine backpressure = %+v", backpressure)
	}
	if status.Attribution.EngineBatchQueueDepth != 3 || status.Attribution.EngineBatchQueue.Completed != 11 || status.Attribution.EngineBatchQueue.Successes != 8 || status.Attribution.EngineBatchQueue.Failures != 2 || status.Attribution.EngineBatchQueue.Cancellations != 1 {
		t.Fatalf("engine batch queue = %+v", status.Attribution)
	}
	if status.Attribution.EngineBatchService.Completed != 10 || status.Attribution.EngineBatchService.Successes != 7 || status.Attribution.EngineBatchService.Failures != 2 || status.Attribution.EngineBatchService.Cancellations != 1 || status.Attribution.EngineBatchService.Active != 1 {
		t.Fatalf("engine batch service = %+v", status.Attribution.EngineBatchService)
	}
	if status.Attribution.ObjectStoreMain.Put.Timing.Attempts != 2 || status.Attribution.ObjectStoreMain.Put.Timing.Active != 1 || status.Attribution.ObjectStoreMain.Put.TransferredBytes != 512 || !status.Attribution.ObjectStoreMain.Put.TransferredBytesAvailable {
		t.Fatalf("main object store = %+v", status.Attribution.ObjectStoreMain)
	}
	if status.Attribution.ObjectStoreWAL.MultipartPart.Timing.Failures != 1 || !status.Attribution.ObjectStoreWAL.MultipartPart.TransferredBytesAvailable || status.Attribution.ObjectStoreMain.MultipartPart.Timing.Failures != 0 {
		t.Fatalf("wal object store = %+v", status.Attribution.ObjectStoreWAL)
	}
	if status.Attribution.ObjectStoreCoordination.Head.Timing.Completed != 3 || status.Attribution.ObjectStoreCoordination.RetryDelayAvailable || status.Attribution.ObjectStoreCoordination.BackgroundPressureAvailable {
		t.Fatalf("coordination object store = %+v", status.Attribution.ObjectStoreCoordination)
	}

	legacy := writerStatus(&vaulticdbv1.WriterStatusResponse{})
	if !reflect.DeepEqual(legacy.Attribution, AttributionSnapshot{}) {
		t.Fatalf("legacy attribution = %+v", legacy.Attribution)
	}
}
