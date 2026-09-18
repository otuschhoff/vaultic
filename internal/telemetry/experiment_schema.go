package telemetry

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"slices"
	"strings"
)

const (
	ExperimentSchemaVersion    = 1
	MaxExperimentDimensions    = 32
	MaxExperimentUnknowns      = 32
	MaxExperimentDelayUS       = 3_600_000_000
	MaxExperimentBandwidth     = 1 << 40
	MaxExperimentWindowMS      = 86_400_000
	MaxExperimentFrequency     = 1_000_000_000
	MaxExperimentBatchSize     = 1_000_000_000
	MaxExperimentEvents        = 1_000_000_000_000
	MaxExperimentDocumentBytes = 1 << 20
)

type DelayMode string

const (
	DelayDisabled        DelayMode = "disabled"
	DelayService         DelayMode = "service"
	DelayDurability      DelayMode = "durability"
	DelayAcknowledgement DelayMode = "acknowledgement"
)

type LatencySemantics string

const (
	LatencyNetworkRTT            LatencySemantics = "network_rtt"
	LatencyTimeToFirstByte       LatencySemantics = "time_to_first_byte"
	LatencyServiceCompletion     LatencySemantics = "service_completion"
	LatencyDurabilityCompletion  LatencySemantics = "durability_completion"
	LatencyAcknowledgementWindow LatencySemantics = "acknowledgement_delivery"
)

type ExperimentProfile struct {
	SchemaVersion   int                 `json:"schema_version"`
	ProfileID       string              `json:"profile_id"`
	Enabled         bool                `json:"enabled"`
	TestOnly        bool                `json:"test_only"`
	Scenario        string              `json:"scenario"`
	Backend         string              `json:"backend"`
	Mode            DelayMode           `json:"mode"`
	Operation       string              `json:"operation"`
	Role            string              `json:"role"`
	Method          string              `json:"method"`
	AccessPattern   string              `json:"access_pattern"`
	TargetID        string              `json:"target_id"`
	ResourceID      string              `json:"resource_id"`
	Placement       string              `json:"placement"`
	Latency         LatencySemantics    `json:"latency_semantics"`
	Interpretation  string              `json:"interpretation"`
	Endpoint        string              `json:"endpoint"`
	Acknowledgement string              `json:"acknowledgement"`
	DelayUS         uint64              `json:"delay_us"`
	JitterUS        uint64              `json:"jitter_us"`
	TailDelayUS     uint64              `json:"tail_delay_us"`
	TailEvery       uint64              `json:"tail_every"`
	CorrelatedFor   uint64              `json:"correlated_for"`
	BandwidthBPS    uint64              `json:"bandwidth_bytes_per_second"`
	Concurrency     uint32              `json:"concurrency"`
	DeadlineMS      uint64              `json:"deadline_ms"`
	MaxRetries      uint32              `json:"max_retries"`
	RetryError      string              `json:"retry_error,omitempty"`
	Seed            uint64              `json:"seed"`
	Holds           []string            `json:"holds,omitempty"`
	Unknowns        []ExperimentUnknown `json:"unknowns,omitempty"`
}

type ExperimentUnknown struct {
	Field  string `json:"field"`
	Reason string `json:"reason"`
}

type ExperimentTarget struct {
	ID         string
	Disposable bool
	Confirmed  bool
}

func (profile ExperimentProfile) ValidateTarget(target ExperimentTarget) error {
	if err := profile.Validate(); err != nil {
		return err
	}
	if profile.Mode == DelayDisabled {
		return nil
	}
	if target.ID != profile.TargetID {
		return fmt.Errorf("experiment target identity does not match profile")
	}
	if !target.Disposable || !target.Confirmed {
		return fmt.Errorf("experiment target must be explicitly confirmed disposable")
	}
	return nil
}

type EvidenceBoolean string

const (
	EvidenceTrue    EvidenceBoolean = "true"
	EvidenceFalse   EvidenceBoolean = "false"
	EvidenceUnknown EvidenceBoolean = "unknown"
)

type ExperimentArtifact struct {
	SchemaVersion                int                 `json:"schema_version"`
	Profile                      ExperimentProfile   `json:"profile"`
	InputIdentitySHA256          string              `json:"input_identity_sha256"`
	InputOrder                   string              `json:"input_order"`
	BinaryRevision               string              `json:"binary_revision"`
	EngineRevision               string              `json:"engine_revision"`
	Hardware                     string              `json:"hardware"`
	RuntimeLimits                string              `json:"runtime_limits"`
	CacheState                   string              `json:"cache_state"`
	Encryption                   string              `json:"encryption"`
	WAL                          string              `json:"wal"`
	BatchSize                    uint64              `json:"batch_size"`
	EffectiveConcurrency         uint32              `json:"effective_concurrency"`
	ObservationWindowMS          uint64              `json:"observation_window_ms"`
	CompletionCriterion          string              `json:"completion_criterion"`
	ResultSHA256                 string              `json:"result_sha256"`
	Outcome                      string              `json:"outcome"`
	Completed                    uint64              `json:"completed"`
	Timeouts                     uint64              `json:"timeouts"`
	Retries                      uint64              `json:"retries"`
	Unfinished                   uint64              `json:"unfinished"`
	SampledDelayUS               uint64              `json:"sampled_delay_us"`
	ObservedDelayUS              uint64              `json:"observed_delay_us"`
	ServerCompletedBeforeTimeout EvidenceBoolean     `json:"server_completed_before_timeout"`
	Unknowns                     []ExperimentUnknown `json:"unknowns,omitempty"`
}

func DecodeExperimentProfile(encoded []byte) (ExperimentProfile, error) {
	var profile ExperimentProfile
	if err := decodeStrictJSON(encoded, &profile); err != nil {
		return ExperimentProfile{}, fmt.Errorf("decode experiment profile: %w", err)
	}
	return profile, profile.Validate()
}

func DecodeExperimentArtifact(encoded []byte) (ExperimentArtifact, error) {
	var artifact ExperimentArtifact
	if err := decodeStrictJSON(encoded, &artifact); err != nil {
		return ExperimentArtifact{}, fmt.Errorf("decode experiment artifact: %w", err)
	}
	return artifact, artifact.Validate()
}

func decodeStrictJSON(encoded []byte, target any) error {
	if len(encoded) > MaxExperimentDocumentBytes {
		return fmt.Errorf("experiment document exceeds %d bytes", MaxExperimentDocumentBytes)
	}
	if err := validateJSONMembers(encoded, reflect.TypeOf(target).Elem()); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return fmt.Errorf("multiple JSON values")
		}
		return err
	}
	return nil
}

func validateJSONMembers(encoded []byte, target reflect.Type) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	var visit func(reflect.Type) error
	visit = func(valueType reflect.Type) error {
		for valueType.Kind() == reflect.Pointer {
			valueType = valueType.Elem()
		}
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delimiter, compound := token.(json.Delim)
		if !compound {
			if token == nil {
				return fmt.Errorf("null is not allowed for %s", valueType)
			}
			return nil
		}
		switch delimiter {
		case '{':
			if valueType.Kind() != reflect.Struct {
				return fmt.Errorf("JSON object does not match schema")
			}
			fields := make(map[string]reflect.Type, valueType.NumField())
			for index := 0; index < valueType.NumField(); index++ {
				field := valueType.Field(index)
				name := strings.Split(field.Tag.Get("json"), ",")[0]
				if name != "" && name != "-" {
					fields[name] = field.Type
				}
			}
			seen := make(map[string]struct{}, len(fields))
			for decoder.More() {
				member, err := decoder.Token()
				if err != nil {
					return err
				}
				name, ok := member.(string)
				if !ok {
					return fmt.Errorf("invalid JSON member")
				}
				fieldType, known := fields[name]
				if !known {
					return fmt.Errorf("unknown field %q", name)
				}
				if _, duplicate := seen[name]; duplicate {
					return fmt.Errorf("duplicate field %q", name)
				}
				seen[name] = struct{}{}
				if err := visit(fieldType); err != nil {
					return err
				}
			}
			_, err = decoder.Token()
			return err
		case '[':
			if valueType.Kind() != reflect.Slice && valueType.Kind() != reflect.Array {
				return fmt.Errorf("JSON array does not match schema")
			}
			for decoder.More() {
				if err := visit(valueType.Elem()); err != nil {
					return err
				}
			}
			_, err = decoder.Token()
			return err
		default:
			return fmt.Errorf("unexpected JSON delimiter %q", delimiter)
		}
	}
	if err := visit(target); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return fmt.Errorf("multiple JSON values")
		}
		return err
	}
	return nil
}

func (profile ExperimentProfile) Validate() error {
	if profile.SchemaVersion != ExperimentSchemaVersion {
		return fmt.Errorf("unsupported experiment schema version %d", profile.SchemaVersion)
	}
	if !validConfiguredID(profile.ProfileID) {
		return fmt.Errorf("invalid experiment profile ID")
	}
	if !profile.TestOnly {
		return fmt.Errorf("experiment profile must be restricted to isolated test targets")
	}
	if err := validateExperimentValue("scenario", profile.Scenario); err != nil {
		return err
	}
	if err := validateExperimentValue("backend", profile.Backend); err != nil {
		return err
	}
	if err := validateExperimentValue("operation", profile.Operation); err != nil {
		return err
	}
	if err := validateExperimentValue("role", profile.Role); err != nil {
		return err
	}
	if !validExperimentOperationRole(profile.Operation, profile.Role) {
		return fmt.Errorf("unsupported experiment operation/role pair %q/%q", profile.Operation, profile.Role)
	}
	if err := validateExperimentValue("method", profile.Method); err != nil {
		return err
	}
	if err := validateExperimentValue("access_pattern", profile.AccessPattern); err != nil {
		return err
	}
	if !validConfiguredID(profile.TargetID) || !validConfiguredID(profile.ResourceID) {
		return fmt.Errorf("invalid experiment target or resource ID")
	}
	if err := validateExperimentValue("placement", profile.Placement); err != nil {
		return err
	}
	if err := validateExperimentValue("interpretation", profile.Interpretation); err != nil {
		return err
	}
	if err := validateExperimentValue("endpoint", profile.Endpoint); err != nil {
		return err
	}
	if err := validateExperimentValue("acknowledgement", profile.Acknowledgement); err != nil {
		return err
	}
	if err := validateScenarioBoundary(profile); err != nil {
		return err
	}
	if !slices.Contains([]DelayMode{DelayDisabled, DelayService, DelayDurability, DelayAcknowledgement}, profile.Mode) {
		return fmt.Errorf("invalid experiment delay mode %q", profile.Mode)
	}
	if !slices.Contains([]LatencySemantics{LatencyNetworkRTT, LatencyTimeToFirstByte, LatencyServiceCompletion, LatencyDurabilityCompletion, LatencyAcknowledgementWindow}, profile.Latency) {
		return fmt.Errorf("invalid experiment latency semantics %q", profile.Latency)
	}
	if profile.Mode == DelayDisabled && (profile.Enabled || profile.DelayUS != 0 || profile.JitterUS != 0 || profile.TailDelayUS != 0 || profile.TailEvery != 0 || profile.CorrelatedFor != 0 || profile.BandwidthBPS != 0 || profile.Concurrency != 0 || profile.DeadlineMS != 0 || profile.MaxRetries != 0 || profile.RetryError != "" && profile.RetryError != "none") {
		return fmt.Errorf("disabled experiment profile contains active injection parameters: enabled=%t delay=%d jitter=%d tail_delay=%d tail_every=%d correlated_for=%d bandwidth=%d concurrency=%d deadline=%d retries=%d retry_error=%q", profile.Enabled, profile.DelayUS, profile.JitterUS, profile.TailDelayUS, profile.TailEvery, profile.CorrelatedFor, profile.BandwidthBPS, profile.Concurrency, profile.DeadlineMS, profile.MaxRetries, profile.RetryError)
	}
	if profile.Mode != DelayDisabled && !profile.Enabled {
		return fmt.Errorf("active experiment delay mode requires enabled=true")
	}
	compatible := profile.Mode == DelayDisabled ||
		profile.Mode == DelayService && (profile.Latency == LatencyTimeToFirstByte || profile.Latency == LatencyServiceCompletion) && (profile.Placement == "inside_capacity" || profile.Placement == "inside_service") ||
		profile.Mode == DelayDurability && profile.Latency == LatencyDurabilityCompletion && profile.Placement == "inside_service" ||
		profile.Mode == DelayAcknowledgement && profile.Latency == LatencyAcknowledgementWindow && profile.Placement == "after_completion"
	if !compatible {
		return fmt.Errorf("experiment delay mode, latency semantics, and placement are incompatible")
	}
	if profile.Scenario == "s3" && profile.Latency == LatencyNetworkRTT && profile.Mode != DelayDisabled {
		return fmt.Errorf("S3 network RTT is an assumption, not an injectable service delay")
	}
	if profile.DelayUS > MaxExperimentDelayUS || profile.JitterUS > MaxExperimentDelayUS || profile.TailDelayUS > MaxExperimentDelayUS {
		return fmt.Errorf("experiment delay exceeds limit %d microseconds", MaxExperimentDelayUS)
	}
	if profile.TailEvery == 0 && profile.TailDelayUS != 0 || profile.TailEvery != 0 && profile.TailDelayUS == 0 {
		return fmt.Errorf("tail delay and frequency must be configured together")
	}
	if profile.TailEvery > MaxExperimentFrequency || profile.CorrelatedFor > MaxExperimentFrequency {
		return fmt.Errorf("experiment tail frequency exceeds limit %d", MaxExperimentFrequency)
	}
	if profile.CorrelatedFor != 0 && profile.TailEvery == 0 {
		return fmt.Errorf("correlated slow period requires a tail frequency")
	}
	if profile.BandwidthBPS > MaxExperimentBandwidth {
		return fmt.Errorf("experiment bandwidth exceeds limit %d", MaxExperimentBandwidth)
	}
	if profile.Concurrency > 4096 {
		return fmt.Errorf("experiment concurrency exceeds limit 4096")
	}
	if profile.DeadlineMS > MaxExperimentWindowMS || profile.MaxRetries > 1_000 {
		return fmt.Errorf("experiment deadline or retry count exceeds limit")
	}
	if len(profile.Holds) > MaxExperimentDimensions {
		return fmt.Errorf("experiment held resources exceed limit %d", MaxExperimentDimensions)
	}
	if len(profile.Holds) == 0 {
		return fmt.Errorf("experiment must explicitly record held resources")
	}
	if err := validateUniqueExperimentValues("hold", profile.Holds); err != nil {
		return err
	}
	if len(profile.Holds) > 1 && slices.Contains(profile.Holds, "none") {
		return fmt.Errorf("experiment hold none is exclusive")
	}
	if profile.Mode == DelayDisabled && (len(profile.Holds) != 1 || profile.Holds[0] != "none") {
		return fmt.Errorf("disabled experiment must hold exactly none")
	}
	if profile.Mode == DelayAcknowledgement && !slices.Contains([]string{"caller", "client_transport"}, profile.Endpoint) {
		return fmt.Errorf("acknowledgement delay requires a caller or client transport endpoint")
	}
	if slices.Contains([]string{"caller", "client_transport"}, profile.Endpoint) {
		for _, hold := range profile.Holds {
			if hold != "none" && hold != "connection" {
				return fmt.Errorf("endpoint %q cannot hold resource %q", profile.Endpoint, hold)
			}
		}
	}
	if profile.RetryError != "" {
		if err := validateExperimentValue("retry", profile.RetryError); err != nil {
			return err
		}
	}
	return validateExperimentUnknowns(profile.Unknowns)
}

func validExperimentOperationRole(operation, role string) bool {
	roles := map[string][]string{
		"backup":        {"source", "repository", "database", "rpc", "cache"},
		"restore":       {"source", "repository", "rpc", "cache", "scratch"},
		"check":         {"repository", "database", "rpc", "cache", "scratch"},
		"legacy_import": {"source", "repository", "database", "wal", "coordination", "rpc", "cache", "scratch"},
		"forget":        {"repository", "database", "rpc"},
		"prune":         {"repository", "database", "rpc"},
	}
	return slices.Contains(roles[operation], role)
}

func validateScenarioBoundary(profile ExperimentProfile) error {
	if profile.Mode == DelayDurability && !slices.Contains([]string{"put", "multipart_complete", "conditional_write", "commit"}, profile.Method) {
		return fmt.Errorf("durability delay requires a persistence operation")
	}
	if profile.Mode == DelayDurability && slices.Contains([]string{"rpc", "cache", "scratch"}, profile.Role) {
		return fmt.Errorf("durability delay requires a storage dependency role")
	}
	if profile.Mode == DelayDurability && slices.Contains([]string{"unknown", "applied", "process_memory", "write_completion"}, profile.Acknowledgement) {
		return fmt.Errorf("durability delay requires a durable acknowledgement")
	}
	roleMethods := map[string][]string{
		"repository":   {"list", "get", "range_get", "head", "put", "multipart_complete", "delete", "conditional_write"},
		"database":     {"get", "range_get", "put", "delete", "scan", "multi_get", "commit"},
		"wal":          {"get", "range_get", "head", "put", "multipart_complete", "delete", "commit"},
		"coordination": {"get", "head", "put", "delete", "conditional_write"},
		"source":       {"open", "read", "stat", "list", "get", "range_get", "head"},
		"scratch":      {"open", "read", "stat", "list", "put", "delete"},
		"cache":        {"get", "range_get", "put", "delete"},
		"rpc":          {"read", "get", "range_get", "head", "list", "scan", "multi_get", "commit"},
	}
	if !slices.Contains(roleMethods[profile.Role], profile.Method) {
		return fmt.Errorf("method is incompatible with role %q", profile.Role)
	}
	acknowledgementMethods := map[string][]string{
		"applied":                  {"commit"},
		"process_memory":           {"put", "commit"},
		"write_completion":         {"put"},
		"sync_stable_storage":      {"put"},
		"object_put_completion":    {"put"},
		"multipart_completion":     {"multipart_complete"},
		"replicated_object_commit": {"put", "conditional_write"},
	}
	if methods, constrained := acknowledgementMethods[profile.Acknowledgement]; constrained && !slices.Contains(methods, profile.Method) {
		return fmt.Errorf("acknowledgement is incompatible with method")
	}
	backendAcknowledgements := []string{"write_completion", "sync_stable_storage", "object_put_completion", "multipart_completion", "replicated_object_commit"}
	if slices.Contains(backendAcknowledgements, profile.Acknowledgement) {
		compatible := map[string]map[string][]string{
			"nfs_hdd": {
				"put": {"write_completion", "sync_stable_storage"},
			},
			"rados_three_replica": {
				"put": {"replicated_object_commit"}, "conditional_write": {"replicated_object_commit"},
			},
			"s3": {
				"put": {"object_put_completion"}, "multipart_complete": {"multipart_completion"},
			},
		}
		if !slices.Contains(compatible[profile.Backend][profile.Method], profile.Acknowledgement) {
			return fmt.Errorf("backend acknowledgement is incompatible with scenario")
		}
	}
	if profile.Role == "rpc" || profile.Role == "cache" || profile.Role == "scratch" {
		return nil
	}
	objectMethod := slices.Contains([]string{"list", "get", "range_get", "head", "put", "multipart_complete", "delete", "conditional_write"}, profile.Method)
	switch profile.Backend {
	case "nfs_hdd":
		if !slices.Contains([]string{"open", "read", "stat", "list", "put", "delete"}, profile.Method) || !slices.Contains([]string{"unknown", "write_completion", "sync_stable_storage"}, profile.Acknowledgement) {
			return fmt.Errorf("NFS method or acknowledgement is incompatible")
		}
	case "rados_three_replica":
		if !objectMethod || !slices.Contains([]string{"unknown", "replicated_object_commit"}, profile.Acknowledgement) {
			return fmt.Errorf("RADOS method or acknowledgement is incompatible")
		}
	case "s3":
		if !objectMethod || !slices.Contains([]string{"unknown", "object_put_completion", "multipart_completion"}, profile.Acknowledgement) {
			return fmt.Errorf("S3 method or acknowledgement is incompatible")
		}
	}
	return nil
}

func (artifact ExperimentArtifact) Validate() error {
	if artifact.SchemaVersion != ExperimentSchemaVersion {
		return fmt.Errorf("unsupported experiment artifact schema version %d", artifact.SchemaVersion)
	}
	if err := artifact.Profile.Validate(); err != nil {
		return err
	}
	for name, value := range map[string]string{
		"binary revision": artifact.BinaryRevision, "engine revision": artifact.EngineRevision,
		"hardware": artifact.Hardware, "runtime limits": artifact.RuntimeLimits,
		"encryption": artifact.Encryption, "WAL": artifact.WAL,
		"completion criterion": artifact.CompletionCriterion,
	} {
		if value == "" || len(value) > MaxMonitorStringLength || bytes.ContainsAny([]byte(value), "\r\n\x00") {
			return fmt.Errorf("invalid experiment artifact %s", name)
		}
	}
	if !validSHA256(artifact.InputIdentitySHA256) || !validSHA256(artifact.ResultSHA256) {
		return fmt.Errorf("experiment artifact requires lowercase SHA-256 identities")
	}
	if err := validateExperimentValue("input_order", artifact.InputOrder); err != nil {
		return err
	}
	if err := validateExperimentValue("cache_state", artifact.CacheState); err != nil {
		return err
	}
	if err := validateExperimentValue("outcome", artifact.Outcome); err != nil {
		return err
	}
	if artifact.ObservationWindowMS == 0 || artifact.ObservationWindowMS > MaxExperimentWindowMS {
		return fmt.Errorf("experiment artifact observation window is outside bounds")
	}
	if artifact.BatchSize > MaxExperimentBatchSize || artifact.EffectiveConcurrency > 4096 {
		return fmt.Errorf("experiment artifact batch size or concurrency exceeds limit")
	}
	if artifact.Completed > MaxExperimentEvents || artifact.Timeouts > MaxExperimentEvents || artifact.Retries > MaxExperimentEvents || artifact.Unfinished > MaxExperimentEvents {
		return fmt.Errorf("experiment artifact event count exceeds limit")
	}
	if artifact.SampledDelayUS > MaxExperimentDelayUS || artifact.ObservedDelayUS > MaxExperimentDelayUS {
		return fmt.Errorf("experiment artifact delay evidence exceeds limit")
	}
	if artifact.Profile.Mode == DelayDisabled && artifact.SampledDelayUS != 0 {
		return fmt.Errorf("disabled experiment cannot contain sampled delay evidence")
	}
	if artifact.Completed != 0 && (artifact.BatchSize == 0 || artifact.EffectiveConcurrency == 0) {
		return fmt.Errorf("completed experiment requires positive batch size and concurrency")
	}
	if artifact.Outcome == "success" && (artifact.Timeouts != 0 || artifact.Unfinished != 0) {
		return fmt.Errorf("successful experiment cannot contain timeouts or unfinished work")
	}
	if artifact.Outcome == "success" && artifact.Completed == 0 {
		return fmt.Errorf("successful experiment requires completed work")
	}
	if artifact.ServerCompletedBeforeTimeout != EvidenceTrue && artifact.ServerCompletedBeforeTimeout != EvidenceFalse && artifact.ServerCompletedBeforeTimeout != EvidenceUnknown {
		return fmt.Errorf("invalid server-completion evidence %q", artifact.ServerCompletedBeforeTimeout)
	}
	if artifact.Outcome == "timeout" && artifact.Timeouts == 0 {
		return fmt.Errorf("timeout outcome requires timeout evidence")
	}
	if artifact.Outcome == "cancellation" && artifact.Unfinished == 0 {
		return fmt.Errorf("cancellation outcome requires unfinished work")
	}
	return validateExperimentUnknowns(artifact.Unknowns)
}

func validateExperimentValue(kind, value string) error {
	var allowed []string
	switch kind {
	case "scenario":
		allowed = []string{"local", "nfs_hdd", "rados_three_replica", "s3"}
	case "backend":
		allowed = []string{"local", "nfs_hdd", "rados_three_replica", "s3", "slatedb", "rpc", "cache", "scratch"}
	case "operation":
		allowed = []string{"backup", "restore", "check", "legacy_import", "forget", "prune"}
	case "role":
		allowed = []string{"repository", "database", "wal", "coordination", "source", "scratch", "cache", "rpc"}
	case "method":
		allowed = []string{"open", "read", "stat", "list", "get", "range_get", "head", "put", "multipart_complete", "delete", "conditional_write", "scan", "multi_get", "commit"}
	case "access_pattern":
		allowed = []string{"not_applicable", "sequential", "random", "mixed"}
	case "placement":
		allowed = []string{"before_admission", "inside_capacity", "inside_service", "after_completion"}
	case "interpretation":
		allowed = []string{"additive", "synthetic"}
	case "endpoint":
		allowed = []string{"caller", "client_transport", "server", "dependency", "storage"}
	case "acknowledgement":
		allowed = []string{"applied", "process_memory", "write_completion", "sync_stable_storage", "object_put_completion", "multipart_completion", "replicated_object_commit", "unknown"}
	case "hold":
		allowed = []string{"none", "admission_slot", "lock", "byte_budget", "connection", "backend_capacity"}
	case "retry":
		allowed = []string{"none", "timeout", "unavailable", "throttled"}
	case "input_order":
		allowed = []string{"canonical", "source", "seeded"}
	case "cache_state":
		allowed = []string{"cold", "warm", "mixed", "not_applicable", "unknown"}
	case "outcome":
		allowed = []string{"success", "failure", "timeout", "cancellation"}
	}
	if !slices.Contains(allowed, value) {
		return fmt.Errorf("invalid or unbounded experiment %s %q", kind, value)
	}
	return nil
}

func validateUniqueExperimentValues(kind string, entries []string) error {
	seen := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		if err := validateExperimentValue(kind, entry); err != nil {
			return err
		}
		if _, exists := seen[entry]; exists {
			return fmt.Errorf("duplicate experiment %s %q", kind, entry)
		}
		seen[entry] = struct{}{}
	}
	return nil
}

func validateExperimentUnknowns(unknowns []ExperimentUnknown) error {
	if len(unknowns) > MaxExperimentUnknowns {
		return fmt.Errorf("experiment unknowns exceed limit %d", MaxExperimentUnknowns)
	}
	seen := make(map[string]struct{}, len(unknowns))
	for _, unknown := range unknowns {
		if err := validateMonitorName("experiment unknown field", unknown.Field); err != nil {
			return err
		}
		if unknown.Reason == "" || len(unknown.Reason) > MaxMonitorStringLength || bytes.ContainsAny([]byte(unknown.Reason), "\r\n\x00") {
			return fmt.Errorf("experiment unknown %q has invalid reason", unknown.Field)
		}
		if _, exists := seen[unknown.Field]; exists {
			return fmt.Errorf("duplicate experiment unknown %q", unknown.Field)
		}
		seen[unknown.Field] = struct{}{}
	}
	return nil
}

func validSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' {
			if character < 'a' || character > 'f' {
				return false
			}
		}
	}
	return true
}
