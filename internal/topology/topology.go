package topology

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/otuschhoff/vaultic/internal/vaultic"
)

const (
	Format   = 1
	MaxBytes = 1 << 20
)

type Document struct {
	Format             uint                  `json:"format"`
	RepositoryID       string                `json:"repository_id"`
	TopologyGeneration uint64                `json:"topology_generation"`
	PackBackends       []PackBackend         `json:"pack_backends"`
	PlacementPolicy    PlacementPolicy       `json:"placement_policy"`
	StagingBackends    []string              `json:"staging_backends"`
	MetadataReplicas   MetadataReplicas      `json:"metadata_replicas"`
	Credentials        map[string]Credential `json:"credentials"`
}

type PackBackend struct {
	ID            string         `json:"id"`
	Provider      Provider       `json:"provider"`
	Endpoint      map[string]any `json:"endpoint"`
	Role          BackendRole    `json:"role"`
	Offsite       bool           `json:"offsite"`
	FailureDomain string         `json:"failure_domain"`
	CredentialRef string         `json:"credential_ref,omitempty"`
}

type Provider string

const (
	ProviderLocal       Provider = "local"
	ProviderS3          Provider = "s3"
	ProviderAzure       Provider = "azure"
	ProviderGCS         Provider = "gcs"
	ProviderGoogleDrive Provider = "google-drive"
)

type BackendRole string

const (
	RolePrimary  BackendRole = "primary"
	RoleStaging  BackendRole = "staging"
	RoleArchival BackendRole = "archival"
	RoleReplica  BackendRole = "replica"
)

type PlacementPolicy struct {
	MinCopies                 uint   `json:"min_copies"`
	MinDomains                uint   `json:"min_domains"`
	MinOffsite                uint   `json:"min_offsite"`
	OffsiteDeadlineSeconds    uint64 `json:"offsite_deadline_seconds"`
	PromotionCrossoverSeconds uint64 `json:"promotion_crossover_seconds"`
}

type MetadataReplicas struct {
	Mode     ReplicaMode                `json:"mode"`
	Order    []string                   `json:"order"`
	Fencing  string                     `json:"fencing"`
	Replicas map[string]MetadataReplica `json:"replicas"`
}

type ReplicaMode string

const (
	ReplicaModeLocal      ReplicaMode = "local"
	ReplicaModeReplicated ReplicaMode = "replicated"
)

type MetadataReplica struct {
	Provider      Provider       `json:"provider"`
	Endpoint      map[string]any `json:"endpoint"`
	CredentialRef string         `json:"credential_ref,omitempty"`
	ReadOnly      bool           `json:"read_only"`
}

type Credential struct {
	Kind               CredentialKind `json:"kind"`
	AccessKeyID        string         `json:"access_key_id,omitempty"`
	SecretAccessKey    string         `json:"secret_access_key,omitempty"`
	AccountName        string         `json:"account_name,omitempty"`
	AccountKey         string         `json:"account_key,omitempty"`
	SASToken           string         `json:"sas_token,omitempty"`
	ServiceAccountJSON string         `json:"service_account_json,omitempty"`
	Subject            string         `json:"subject,omitempty"`
	ClientID           string         `json:"client_id,omitempty"`
	ClientSecret       string         `json:"client_secret,omitempty"`
	RefreshToken       string         `json:"refresh_token,omitempty"`
	Scopes             []string       `json:"scopes,omitempty"`
	TokenURI           string         `json:"token_uri,omitempty"`
	IssuedAt           string         `json:"issued_at,omitempty"`
	RotationDue        string         `json:"rotation_due,omitempty"`
	MayIssue           *MayIssue      `json:"may_issue,omitempty"`
}

type CredentialKind string

const (
	CredentialAWSStatic             CredentialKind = "aws-static"
	CredentialS3Static              CredentialKind = "s3-static"
	CredentialAzureSharedKey        CredentialKind = "azure-shared-key"
	CredentialAzureSAS              CredentialKind = "azure-sas"
	CredentialGCPServiceAccountJSON CredentialKind = "gcp-service-account-json"
	CredentialOAuth2RefreshToken    CredentialKind = "oauth2-refresh-token"
	CredentialNone                  CredentialKind = "none"
)

type MayIssue struct {
	RoleARN        string `json:"role_arn,omitempty"`
	ServiceAccount string `json:"service_account,omitempty"`
}

type Mutation struct {
	Operation  string           `json:"operation"`
	Backend    *PackBackend     `json:"backend,omitempty"`
	ID         string           `json:"id,omitempty"`
	Replica    *MetadataReplica `json:"replica,omitempty"`
	Reference  string           `json:"reference,omitempty"`
	Credential *Credential      `json:"credential,omitempty"`
}

type ConflictError struct {
	Field string
}

func (err *ConflictError) Error() string {
	return fmt.Sprintf("capsule topology conflicts with repository config field %s", err.Field)
}

func (document Document) ValidateConfig(config vaultic.Config) error {
	if len(config.PlacementBackends) != len(document.PackBackends) {
		return &ConflictError{Field: "placement_backends"}
	}
	for index, backend := range document.PackBackends {
		configured := config.PlacementBackends[index]
		if configured.ID != backend.ID || configured.Role != string(backend.Role) ||
			configured.Offsite != backend.Offsite || configured.FailureDomain != backend.FailureDomain {
			return &ConflictError{Field: "placement_backends"}
		}
	}
	policy := config.PlacementPolicy
	if policy.MinCopies != document.PlacementPolicy.MinCopies ||
		policy.MinDomains != document.PlacementPolicy.MinDomains ||
		policy.MinOffsite != document.PlacementPolicy.MinOffsite ||
		policy.OffsiteDeadline != int64(document.PlacementPolicy.OffsiteDeadlineSeconds) ||
		policy.PromotionCrossoverSeconds != int64(document.PlacementPolicy.PromotionCrossoverSeconds) {
		return &ConflictError{Field: "placement_policy"}
	}
	if !slices.Equal(config.StagingBackends, document.StagingBackends) {
		return &ConflictError{Field: "staging_backends"}
	}
	return nil
}

func Decode(encoded []byte) (Document, error) {
	return decode(encoded, false)
}

func DecodeRedacted(encoded []byte) (Document, error) {
	return decode(encoded, true)
}

func decode(encoded []byte, redacted bool) (Document, error) {
	if len(encoded) > MaxBytes {
		return Document{}, fmt.Errorf("sealed topology exceeds %d bytes", MaxBytes)
	}
	var document Document
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&document); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return Document{}, fmt.Errorf("decode sealed topology")
	}
	if err := document.validate(redacted); err != nil {
		return Document{}, err
	}
	canonical, err := json.Marshal(document)
	if err != nil {
		return Document{}, fmt.Errorf("encode sealed topology: %w", err)
	}
	if !bytes.Equal(canonical, encoded) {
		return Document{}, fmt.Errorf("sealed topology is not canonical JSON")
	}
	return document, nil
}

func (document Document) CanonicalJSON() ([]byte, error) {
	if err := document.Validate(); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(document)
	if err != nil {
		return nil, fmt.Errorf("encode sealed topology: %w", err)
	}
	if len(encoded) > MaxBytes {
		return nil, fmt.Errorf("sealed topology exceeds %d bytes", MaxBytes)
	}
	return encoded, nil
}

func (document Document) SHA256() (string, error) {
	encoded, err := document.CanonicalJSON()
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func (document Document) RedactedJSON() ([]byte, error) {
	redacted := document
	redacted.Credentials = nil
	return json.Marshal(redacted)
}

func (document Document) Validate() error {
	return document.validate(false)
}

func (document Document) validate(redacted bool) error {
	if document.Format != Format || document.RepositoryID == "" || document.TopologyGeneration == 0 || len(document.PackBackends) == 0 {
		return fmt.Errorf("invalid sealed topology identity or version")
	}
	backendIDs := make(map[string]struct{}, len(document.PackBackends))
	domains := make(map[string]struct{})
	offsite := uint(0)
	references := make(map[string]struct{})
	for index, item := range document.PackBackends {
		if item.ID == "" || item.FailureDomain == "" {
			return fmt.Errorf("pack backend identity is incomplete")
		}
		if _, duplicate := backendIDs[item.ID]; duplicate || index > 0 && document.PackBackends[index-1].ID >= item.ID {
			return fmt.Errorf("pack backends require unique IDs in canonical order")
		}
		backendIDs[item.ID] = struct{}{}
		domains[item.FailureDomain] = struct{}{}
		if item.Offsite {
			offsite++
		}
		if err := validateEndpoint(item.Provider, item.Endpoint); err != nil {
			return err
		}
		if err := validateReference(item.Provider, item.CredentialRef, references); err != nil {
			return err
		}
	}
	policy := document.PlacementPolicy
	if policy.MinCopies == 0 || policy.MinDomains == 0 || uint(len(document.PackBackends)) < policy.MinCopies || uint(len(domains)) < policy.MinDomains || offsite < policy.MinOffsite {
		return fmt.Errorf("pack backends cannot satisfy placement policy")
	}
	seenStaging := make(map[string]struct{}, len(document.StagingBackends))
	for index, id := range document.StagingBackends {
		_, exists := backendIDs[id]
		_, duplicate := seenStaging[id]
		if !exists || duplicate || index > 0 && document.StagingBackends[index-1] >= id {
			return fmt.Errorf("staging backends must be known, unique, and canonically ordered")
		}
		seenStaging[id] = struct{}{}
	}
	if err := document.MetadataReplicas.validate(references); err != nil {
		return err
	}
	for reference, credential := range document.Credentials {
		if !validReference(reference) {
			return fmt.Errorf("invalid credential reference %q", reference)
		}
		if _, used := references[reference]; !used {
			return fmt.Errorf("unused credential reference %q", reference)
		}
		if err := credential.Validate(); err != nil {
			return fmt.Errorf("credential %q: %w", reference, err)
		}
	}
	for reference := range references {
		credential, exists := document.Credentials[reference]
		if !exists && !redacted {
			return fmt.Errorf("dangling credential reference %q", reference)
		}
		if exists {
			for _, item := range document.PackBackends {
				if item.CredentialRef == reference && !credentialSupportsProvider(credential.Kind, item.Provider) {
					return fmt.Errorf("credential %q kind %q cannot authenticate provider %q", reference, credential.Kind, item.Provider)
				}
			}
			for _, replica := range document.MetadataReplicas.Replicas {
				if replica.CredentialRef == reference && !credentialSupportsProvider(credential.Kind, replica.Provider) {
					return fmt.Errorf("credential %q kind %q cannot authenticate provider %q", reference, credential.Kind, replica.Provider)
				}
			}
		}
	}
	if redacted && len(document.Credentials) != 0 {
		return fmt.Errorf("redacted topology contains credentials")
	}
	return nil
}

func (replicas MetadataReplicas) validate(references map[string]struct{}) error {
	if len(replicas.Order) == 0 || len(replicas.Replicas) == 0 || replicas.Fencing == "" {
		return fmt.Errorf("metadata replica topology is incomplete")
	}
	seen := make(map[string]struct{}, len(replicas.Order))
	for _, id := range replicas.Order {
		if _, duplicate := seen[id]; duplicate {
			return fmt.Errorf("duplicate metadata replica %q", id)
		}
		seen[id] = struct{}{}
	}
	if len(seen) != len(replicas.Replicas) {
		return fmt.Errorf("metadata replica order is incomplete")
	}
	if _, exists := seen[replicas.Fencing]; !exists {
		return fmt.Errorf("metadata fencing replica is unknown")
	}
	if replicas.Mode == ReplicaModeLocal && len(replicas.Replicas) != 1 || replicas.Mode != ReplicaModeLocal && replicas.Mode != ReplicaModeReplicated {
		return fmt.Errorf("invalid metadata replica mode")
	}
	for id, replica := range replicas.Replicas {
		if _, exists := seen[id]; !exists {
			return fmt.Errorf("unordered metadata replica %q", id)
		}
		if err := validateEndpoint(replica.Provider, replica.Endpoint); err != nil {
			return err
		}
		if err := validateReference(replica.Provider, replica.CredentialRef, references); err != nil {
			return err
		}
	}
	return nil
}

func (credential Credential) Validate() error {
	valid := false
	switch credential.Kind {
	case CredentialAWSStatic, CredentialS3Static:
		valid = credential.AccessKeyID != "" && credential.SecretAccessKey != ""
	case CredentialAzureSharedKey:
		valid = credential.AccountName != "" && credential.AccountKey != ""
	case CredentialAzureSAS:
		valid = credential.SASToken != ""
	case CredentialGCPServiceAccountJSON:
		valid = credential.ServiceAccountJSON != "" && json.Valid([]byte(credential.ServiceAccountJSON))
	case CredentialOAuth2RefreshToken:
		valid = credential.ClientID != "" && credential.ClientSecret != "" && credential.RefreshToken != "" && credential.TokenURI != "" && len(credential.Scopes) > 0 && !slices.Contains(credential.Scopes, "")
	case CredentialNone:
		valid = !credential.HasSecret()
	}
	if !valid {
		return fmt.Errorf("fields do not match credential kind")
	}
	return nil
}

func (credential Credential) HasSecret() bool {
	return credential.SecretAccessKey != "" || credential.AccountKey != "" || credential.SASToken != "" || credential.ServiceAccountJSON != "" || credential.ClientSecret != "" || credential.RefreshToken != ""
}

func credentialSupportsProvider(kind CredentialKind, provider Provider) bool {
	switch provider {
	case ProviderS3:
		return kind == CredentialAWSStatic || kind == CredentialS3Static || kind == CredentialNone
	case ProviderAzure:
		return kind == CredentialAzureSharedKey || kind == CredentialAzureSAS || kind == CredentialNone
	case ProviderGCS:
		return kind == CredentialGCPServiceAccountJSON || kind == CredentialNone
	case ProviderGoogleDrive:
		return kind == CredentialOAuth2RefreshToken || kind == CredentialGCPServiceAccountJSON
	default:
		return false
	}
}

func validateReference(provider Provider, reference string, references map[string]struct{}) error {
	if provider == ProviderLocal {
		if reference != "" {
			return fmt.Errorf("local endpoints must not reference credentials")
		}
		return nil
	}
	if !validReference(reference) {
		return fmt.Errorf("remote endpoint requires a valid credential reference")
	}
	references[reference] = struct{}{}
	return nil
}

func validReference(reference string) bool {
	name, ok := strings.CutPrefix(reference, "cred:")
	if !ok || name == "" {
		return false
	}
	for _, character := range name {
		if !(character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || strings.ContainsRune("-_.", character)) {
			return false
		}
	}
	return true
}

func validateEndpoint(provider Provider, endpoint map[string]any) error {
	requiredByProvider := map[Provider][]string{
		ProviderLocal:       {"data_dir"},
		ProviderS3:          {"url", "bucket", "region"},
		ProviderAzure:       {"url", "container"},
		ProviderGCS:         {"bucket"},
		ProviderGoogleDrive: {"drive_id", "root_folder_id", "path"},
	}
	allowedByProvider := map[Provider]map[string]struct{}{
		ProviderLocal:       {"data_dir": {}},
		ProviderS3:          {"url": {}, "bucket": {}, "prefix": {}, "region": {}, "storage_class": {}, "tls_sha256": {}},
		ProviderAzure:       {"url": {}, "container": {}, "prefix": {}, "tls_sha256": {}},
		ProviderGCS:         {"bucket": {}, "prefix": {}},
		ProviderGoogleDrive: {"drive_id": {}, "root_folder_id": {}, "path": {}},
	}
	required := requiredByProvider[provider]
	if required == nil {
		return fmt.Errorf("unsupported endpoint provider %q", provider)
	}
	for _, field := range required {
		value, ok := endpoint[field].(string)
		if !ok || value == "" {
			return fmt.Errorf("%s endpoint requires non-empty %s", provider, field)
		}
	}
	allowed := allowedByProvider[provider]
	for field, value := range endpoint {
		if _, ok := allowed[field]; !ok {
			return fmt.Errorf("%s endpoint contains unsupported field %q", provider, field)
		}
		if value == nil && field == "tls_sha256" {
			continue
		}
		switch value.(type) {
		case string:
		default:
			return fmt.Errorf("%s endpoint field %q must be a string", provider, field)
		}
	}
	return nil
}
