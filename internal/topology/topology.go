// Package topology defines sealed repository storage topology documents.
package topology

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/otuschhoff/vaultic/internal/backend/s3"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

const (
	Format   = 2
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
	ID               string            `json:"id"`
	Provider         Provider          `json:"provider"`
	Endpoint         map[string]any    `json:"endpoint"`
	Role             BackendRole       `json:"role"`
	Offsite          bool              `json:"offsite"`
	FailureDomain    string            `json:"failure_domain"`
	CredentialPolicy *CredentialPolicy `json:"credential_policy,omitempty"`
}

type CredentialBindings struct {
	StorageRead     string `json:"storage-read,omitempty"`
	StorageAppend   string `json:"storage-append,omitempty"`
	StorageMaintain string `json:"storage-maintain,omitempty"`
	StorageLock     string `json:"storage-lock,omitempty"`
}

type CredentialPolicy struct {
	STS                 *S3STSPolicy               `json:"sts,omitempty"`
	AzureUserDelegation *AzureUserDelegationPolicy `json:"azure_user_delegation,omitempty"`
	GCPDownscope        *GCPDownscopePolicy        `json:"gcp_downscope,omitempty"`
	Static              *StaticCredentialPolicy    `json:"static,omitempty"`
}

type S3STSPolicy struct {
	IssuerRef   string             `json:"issuer_ref"`
	Endpoint    string             `json:"endpoint"`
	Region      string             `json:"region"`
	SessionName string             `json:"session_name"`
	ExternalID  string             `json:"external_id,omitempty"`
	Roles       CredentialBindings `json:"roles"`
	Fallback    STSFallback        `json:"fallback"`
}

type AzureUserDelegationPolicy struct {
	IssuerRef             string                  `json:"issuer_ref"`
	ServiceVersion        string                  `json:"service_version"`
	SignedIP              string                  `json:"signed_ip,omitempty"`
	HierarchicalNamespace bool                    `json:"hierarchical_namespace"`
	Tiers                 []StorageCredentialTier `json:"tiers"`
	Fallback              STSFallback             `json:"fallback"`
}

type GCPDownscopePolicy struct {
	IssuerRef              string                  `json:"issuer_ref"`
	ServiceAccount         string                  `json:"service_account"`
	IAMCredentialsEndpoint string                  `json:"iam_credentials_endpoint"`
	TokenExchangeEndpoint  string                  `json:"token_exchange_endpoint"`
	Tiers                  []StorageCredentialTier `json:"tiers"`
	Fallback               STSFallback             `json:"fallback"`
}

type StaticCredentialPolicy struct {
	Generation         uint64             `json:"generation"`
	RevokedGenerations []uint64           `json:"revoked_generations,omitempty"`
	Bindings           CredentialBindings `json:"bindings"`
}

type STSFallback string

const (
	STSNoFallback          STSFallback = "disabled"
	STSStaticOnUnavailable STSFallback = "static-on-unavailable"
)

type StorageCredentialTier string

const (
	StorageRead     StorageCredentialTier = "storage-read"
	StorageAppend   StorageCredentialTier = "storage-append"
	StorageMaintain StorageCredentialTier = "storage-maintain"
	StorageLock     StorageCredentialTier = "storage-lock"
)

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
	Provider         Provider          `json:"provider"`
	Endpoint         map[string]any    `json:"endpoint"`
	CredentialPolicy *CredentialPolicy `json:"credential_policy,omitempty"`
	ReadOnly         bool              `json:"read_only"`
}

type Credential struct {
	Kind               CredentialKind `json:"kind"`
	AccessKeyID        string         `json:"access_key_id,omitempty"`
	SecretAccessKey    string         `json:"secret_access_key,omitempty"`
	SessionToken       string         `json:"session_token,omitempty"`
	ExpiresAt          string         `json:"expires_at,omitempty"`
	AccountName        string         `json:"account_name,omitempty"`
	AccountKey         string         `json:"account_key,omitempty"`
	SASToken           string         `json:"sas_token,omitempty"`
	ServiceAccountJSON string         `json:"service_account_json,omitempty"`
	AccessToken        string         `json:"access_token,omitempty"`
	Subject            string         `json:"subject,omitempty"`
	TenantID           string         `json:"tenant_id,omitempty"`
	ClientID           string         `json:"client_id,omitempty"`
	ClientSecret       string         `json:"client_secret,omitempty"`
	RefreshToken       string         `json:"refresh_token,omitempty"`
	Scopes             []string       `json:"scopes,omitempty"`
	TokenURI           string         `json:"token_uri,omitempty"`
	IssuedAt           string         `json:"issued_at,omitempty"`
	RotationDue        string         `json:"rotation_due,omitempty"`
}

type CredentialKind string

const (
	CredentialAWSStatic              CredentialKind = "aws-static"
	CredentialS3Static               CredentialKind = "s3-static"
	CredentialS3Session              CredentialKind = "s3-session"
	CredentialAzureSharedKey         CredentialKind = "azure-shared-key"
	CredentialAzureSAS               CredentialKind = "azure-sas"
	CredentialAzureEntraClientSecret CredentialKind = "azure-entra-client-secret"
	CredentialGCPServiceAccountJSON  CredentialKind = "gcp-service-account-json"
	CredentialGCPAccessToken         CredentialKind = "gcp-access-token"
	CredentialOAuth2RefreshToken     CredentialKind = "oauth2-refresh-token"
	CredentialNone                   CredentialKind = "none"
)

type Mutation struct {
	Operation   string                `json:"operation"`
	Backend     *PackBackend          `json:"backend,omitempty"`
	ID          string                `json:"id,omitempty"`
	Replica     *MetadataReplica      `json:"replica,omitempty"`
	Reference   string                `json:"reference,omitempty"`
	Credential  *Credential           `json:"credential,omitempty"`
	Credentials map[string]Credential `json:"credentials,omitempty"`
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
	backendIDs, domains, offsite, references, err := validatePackBackends(document.PackBackends)
	if err != nil {
		return err
	}
	policy := document.PlacementPolicy
	if policy.MinCopies == 0 || policy.MinDomains == 0 ||
		uint(len(document.PackBackends)) < policy.MinCopies ||
		uint(len(domains)) < policy.MinDomains || offsite < policy.MinOffsite {
		return fmt.Errorf("pack backends cannot satisfy placement policy")
	}
	if err := validateStagingBackends(document.StagingBackends, backendIDs); err != nil {
		return err
	}
	if err := document.MetadataReplicas.validate(references); err != nil {
		return err
	}
	return document.validateCredentials(references, redacted)
}

func validatePackBackends(
	backends []PackBackend,
) (map[string]struct{}, map[string]struct{}, uint, map[string]struct{}, error) {
	backendIDs := make(map[string]struct{}, len(backends))
	domains := make(map[string]struct{})
	offsite := uint(0)
	references := make(map[string]struct{})
	for index, item := range backends {
		if item.ID == "" || item.FailureDomain == "" {
			return nil, nil, 0, nil, fmt.Errorf("pack backend identity is incomplete")
		}
		if _, duplicate := backendIDs[item.ID]; duplicate || index > 0 && backends[index-1].ID >= item.ID {
			return nil, nil, 0, nil, fmt.Errorf("pack backends require unique IDs in canonical order")
		}
		backendIDs[item.ID] = struct{}{}
		domains[item.FailureDomain] = struct{}{}
		if item.Offsite {
			offsite++
		}
		if err := validateEndpoint(item.Provider, item.Endpoint); err != nil {
			return nil, nil, 0, nil, err
		}
		if err := validateCredentialPolicy(item.Provider, item.Endpoint, item.CredentialPolicy, references); err != nil {
			return nil, nil, 0, nil, err
		}
	}
	return backendIDs, domains, offsite, references, nil
}

func validateStagingBackends(staging []string, backendIDs map[string]struct{}) error {
	seenStaging := make(map[string]struct{}, len(staging))
	for index, id := range staging {
		_, exists := backendIDs[id]
		_, duplicate := seenStaging[id]
		if !exists || duplicate || index > 0 && staging[index-1] >= id {
			return fmt.Errorf("staging backends must be known, unique, and canonically ordered")
		}
		seenStaging[id] = struct{}{}
	}
	return nil
}

func (document Document) validateCredentials(references map[string]struct{}, redacted bool) error {
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
		if credential.Kind == CredentialS3Session || credential.Kind == CredentialGCPAccessToken {
			return fmt.Errorf("credential %q: temporary credentials must not be stored in topology", reference)
		}
	}
	for reference := range references {
		credential, exists := document.Credentials[reference]
		if !exists && !redacted {
			return fmt.Errorf("dangling credential reference %q", reference)
		}
		if exists {
			if err := document.validateCredentialProviders(reference, credential); err != nil {
				return err
			}
		}
	}
	if redacted && len(document.Credentials) != 0 {
		return fmt.Errorf("redacted topology contains credentials")
	}
	return nil
}

func (document Document) validateCredentialProviders(reference string, credential Credential) error {
	for _, item := range document.PackBackends {
		if err := validateCredentialProviderForPolicy(
			reference, credential, item.Provider, item.CredentialPolicy,
		); err != nil {
			return err
		}
	}
	for _, replica := range document.MetadataReplicas.Replicas {
		if err := validateCredentialProviderForPolicy(
			reference, credential, replica.Provider, replica.CredentialPolicy,
		); err != nil {
			return err
		}
	}
	return nil
}

func validateCredentialProviderForPolicy(
	reference string, credential Credential, provider Provider, policy *CredentialPolicy,
) error {
	stsIssuer := policy != nil && policy.STS != nil && policy.STS.IssuerRef == reference
	azureIssuer := policy != nil && policy.AzureUserDelegation != nil &&
		policy.AzureUserDelegation.IssuerRef == reference
	gcpIssuer := policy != nil && policy.GCPDownscope != nil && policy.GCPDownscope.IssuerRef == reference
	if stsIssuer && credential.Kind != CredentialAWSStatic && credential.Kind != CredentialS3Static {
		return fmt.Errorf("STS issuer credential %q must be a static S3 credential", reference)
	}
	if azureIssuer && credential.Kind != CredentialAzureEntraClientSecret {
		return fmt.Errorf("azure user delegation issuer credential %q must be an Entra client secret", reference)
	}
	if gcpIssuer && credential.Kind != CredentialGCPServiceAccountJSON {
		return fmt.Errorf("GCP downscope issuer credential %q must be a service account JSON credential", reference)
	}
	if credentialReferenceUsed(policy, reference) && !stsIssuer && !azureIssuer && !gcpIssuer &&
		!credentialSupportsProvider(credential.Kind, provider) {
		return fmt.Errorf(
			"credential %q kind %q cannot authenticate provider %q",
			reference, credential.Kind, provider,
		)
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
		if err := validateCredentialPolicy(replica.Provider, replica.Endpoint, replica.CredentialPolicy, references); err != nil {
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
	case CredentialS3Session:
		valid = credential.AccessKeyID != "" && credential.SecretAccessKey != "" &&
			credential.SessionToken != "" && credential.ExpiresAt != ""
	case CredentialAzureSharedKey:
		valid = credential.AccountName != "" && credential.AccountKey != ""
	case CredentialAzureSAS:
		valid = credential.SASToken != ""
	case CredentialAzureEntraClientSecret:
		valid = credential.TenantID != "" && credential.ClientID != "" &&
			credential.ClientSecret != "" && credential.TokenURI != "" &&
			len(credential.Scopes) == 1 && credential.Scopes[0] != ""
	case CredentialGCPServiceAccountJSON:
		valid = credential.ServiceAccountJSON != "" && json.Valid([]byte(credential.ServiceAccountJSON))
	case CredentialGCPAccessToken:
		valid = credential.AccessToken != "" && credential.ExpiresAt != ""
	case CredentialOAuth2RefreshToken:
		valid = credential.ClientID != "" && credential.ClientSecret != "" &&
			credential.RefreshToken != "" && credential.TokenURI != "" &&
			len(credential.Scopes) > 0 && !slices.Contains(credential.Scopes, "")
	case CredentialNone:
		valid = !credential.HasSecret()
	}
	if !valid {
		return fmt.Errorf("fields do not match credential kind")
	}
	return nil
}

func (credential Credential) HasSecret() bool {
	return credential.SecretAccessKey != "" || credential.AccountKey != "" || credential.SASToken != "" ||
		credential.ServiceAccountJSON != "" || credential.AccessToken != "" || credential.ClientSecret != "" || credential.RefreshToken != "" ||
		credential.SessionToken != ""
}

func credentialSupportsProvider(kind CredentialKind, provider Provider) bool {
	switch provider {
	case ProviderS3:
		return kind == CredentialAWSStatic || kind == CredentialS3Static || kind == CredentialS3Session || kind == CredentialNone
	case ProviderAzure:
		return kind == CredentialAzureSharedKey || kind == CredentialAzureSAS || kind == CredentialNone
	case ProviderGCS:
		return kind == CredentialGCPServiceAccountJSON || kind == CredentialGCPAccessToken || kind == CredentialNone
	case ProviderGoogleDrive:
		return kind == CredentialOAuth2RefreshToken || kind == CredentialGCPServiceAccountJSON
	default:
		return false
	}
}

func (bindings CredentialBindings) Reference(tier StorageCredentialTier) string {
	switch tier {
	case StorageRead:
		return bindings.StorageRead
	case StorageAppend:
		return bindings.StorageAppend
	case StorageMaintain:
		return bindings.StorageMaintain
	case StorageLock:
		return bindings.StorageLock
	default:
		return ""
	}
}

func (bindings CredentialBindings) references() []string {
	return []string{bindings.StorageRead, bindings.StorageAppend, bindings.StorageMaintain, bindings.StorageLock}
}

func SelectStaticCredentialReference(policy *CredentialPolicy, tier StorageCredentialTier) (string, error) {
	if policy == nil || policy.Static == nil {
		return "", fmt.Errorf("credential binding %q is not configured", tier)
	}
	reference := policy.Static.Bindings.Reference(tier)
	if reference == "" {
		return "", fmt.Errorf("credential binding %q is not configured", tier)
	}
	return reference, nil
}

func credentialReferenceUsed(policy *CredentialPolicy, reference string) bool {
	if policy == nil {
		return false
	}
	if policy.STS != nil && policy.STS.IssuerRef == reference {
		return true
	}
	if policy.AzureUserDelegation != nil && policy.AzureUserDelegation.IssuerRef == reference {
		return true
	}
	if policy.GCPDownscope != nil && policy.GCPDownscope.IssuerRef == reference {
		return true
	}
	if policy.Static == nil {
		return false
	}
	return slices.Contains(policy.Static.Bindings.references(), reference)
}

func validateCredentialPolicy(
	provider Provider, endpoint map[string]any, policy *CredentialPolicy, references map[string]struct{},
) error {
	if provider == ProviderLocal {
		if policy != nil {
			return fmt.Errorf("local endpoints must not reference credentials")
		}
		return nil
	}
	if policy == nil || policy.STS == nil && policy.AzureUserDelegation == nil && policy.GCPDownscope == nil && policy.Static == nil {
		return fmt.Errorf("remote endpoint requires a valid credential reference")
	}
	if dynamicPolicyCount(policy) > 1 {
		return fmt.Errorf("credential policy must configure only one dynamic issuer")
	}
	if policy.STS != nil {
		if provider != ProviderS3 {
			return fmt.Errorf("STS credential policy requires an S3 endpoint")
		}
		if err := validateSTSPolicy(*policy.STS, endpoint); err != nil {
			return err
		}
		references[policy.STS.IssuerRef] = struct{}{}
	}
	if policy.AzureUserDelegation != nil {
		if err := validateAzureUserDelegationPolicy(provider, endpoint, *policy.AzureUserDelegation); err != nil {
			return err
		}
		references[policy.AzureUserDelegation.IssuerRef] = struct{}{}
	}
	if policy.GCPDownscope != nil {
		if err := validateGCPDownscopePolicy(provider, *policy.GCPDownscope); err != nil {
			return err
		}
		references[policy.GCPDownscope.IssuerRef] = struct{}{}
	}
	if err := validateCredentialFallback(policy); err != nil {
		return err
	}
	if policy.Static == nil {
		return nil
	}
	return validateStaticCredentialPolicy(*policy.Static, references)
}

func dynamicPolicyCount(policy *CredentialPolicy) int {
	count := 0
	if policy.STS != nil {
		count++
	}
	if policy.AzureUserDelegation != nil {
		count++
	}
	if policy.GCPDownscope != nil {
		count++
	}
	return count
}

func validateCredentialFallback(policy *CredentialPolicy) error {
	issuerRef := ""
	var fallback STSFallback
	var required []StorageCredentialTier
	if policy.STS != nil {
		issuerRef = policy.STS.IssuerRef
		fallback = policy.STS.Fallback
		for index, role := range policy.STS.Roles.references() {
			if role != "" {
				required = append(required, []StorageCredentialTier{StorageRead, StorageAppend, StorageMaintain, StorageLock}[index])
			}
		}
	} else if policy.AzureUserDelegation != nil {
		issuerRef = policy.AzureUserDelegation.IssuerRef
		fallback = policy.AzureUserDelegation.Fallback
		required = policy.AzureUserDelegation.Tiers
	} else if policy.GCPDownscope != nil {
		issuerRef = policy.GCPDownscope.IssuerRef
		fallback = policy.GCPDownscope.Fallback
		required = policy.GCPDownscope.Tiers
	} else {
		return nil
	}
	if policy.Static != nil && slices.Contains(policy.Static.Bindings.references(), issuerRef) {
		return fmt.Errorf("dynamic issuer credential must not be a consumer static binding")
	}
	if fallback != STSStaticOnUnavailable {
		return nil
	}
	if policy.Static == nil {
		return fmt.Errorf("static-on-unavailable requires a static credential policy")
	}
	for _, tier := range required {
		if policy.Static.Bindings.Reference(tier) == "" {
			return fmt.Errorf("static-on-unavailable requires an exact static binding for every dynamic tier")
		}
	}
	return nil
}

func validateGCPDownscopePolicy(provider Provider, policy GCPDownscopePolicy) error {
	if provider != ProviderGCS {
		return fmt.Errorf("GCP downscope policy requires a GCS endpoint")
	}
	if !validReference(policy.IssuerRef) || !validGCPServiceAccount(policy.ServiceAccount) || len(policy.Tiers) == 0 {
		return fmt.Errorf("GCP downscope policy is incomplete")
	}
	if policy.Fallback != STSNoFallback && policy.Fallback != STSStaticOnUnavailable {
		return fmt.Errorf("invalid GCP downscope fallback policy %q", policy.Fallback)
	}
	for index, tier := range policy.Tiers {
		if storageTierRank(tier) == 0 || index > 0 && storageTierRank(policy.Tiers[index-1]) >= storageTierRank(tier) {
			return fmt.Errorf("GCP downscope tiers must be valid, unique, and canonically ordered")
		}
	}
	for name, configured := range map[string][2]string{
		"IAM credentials": {policy.IAMCredentialsEndpoint, "https://iamcredentials.googleapis.com"},
		"token exchange":  {policy.TokenExchangeEndpoint, "https://sts.googleapis.com/v1/token"},
	} {
		raw, production := configured[0], configured[1]
		parsed, err := url.Parse(raw)
		loopbackHTTP := parsed != nil && parsed.Scheme == "http" &&
			(parsed.Hostname() == "127.0.0.1" || parsed.Hostname() == "::1" || parsed.Hostname() == "localhost")
		if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" ||
			(raw != production && !loopbackHTTP) {
			return fmt.Errorf("GCP %s endpoint is invalid", name)
		}
	}
	return nil
}

func validGCPServiceAccount(value string) bool {
	name, ok := strings.CutSuffix(value, ".iam.gserviceaccount.com")
	if !ok || name == "" || strings.Count(name, "@") != 1 {
		return false
	}
	for _, character := range name {
		if !validReferenceCharacter(character) && character != '@' {
			return false
		}
	}
	return true
}

func validateAzureUserDelegationPolicy(provider Provider, endpoint map[string]any, policy AzureUserDelegationPolicy) error {
	if provider != ProviderAzure {
		return fmt.Errorf("azure user delegation policy requires an Azure endpoint")
	}
	if !validReference(policy.IssuerRef) || !validServiceVersion(policy.ServiceVersion) || len(policy.Tiers) == 0 {
		return fmt.Errorf("azure user delegation policy is incomplete")
	}
	if prefix, _ := endpoint["prefix"].(string); prefix != "" {
		return fmt.Errorf("azure user delegation requires an empty prefix and dedicated container")
	}
	if policy.Fallback != STSNoFallback && policy.Fallback != STSStaticOnUnavailable {
		return fmt.Errorf("invalid Azure user delegation fallback policy %q", policy.Fallback)
	}
	for index, tier := range policy.Tiers {
		if storageTierRank(tier) == 0 ||
			index > 0 && storageTierRank(policy.Tiers[index-1]) >= storageTierRank(tier) {
			return fmt.Errorf("azure user delegation tiers must be valid, unique, and canonically ordered")
		}
	}
	if slices.Contains(policy.Tiers, StorageLock) && !policy.HierarchicalNamespace {
		return fmt.Errorf("azure storage-lock user delegation requires hierarchical namespace")
	}
	if policy.SignedIP != "" {
		parts := strings.Split(policy.SignedIP, "-")
		if len(parts) > 2 || slices.ContainsFunc(parts, func(value string) bool {
			ip := net.ParseIP(value)
			return ip == nil || ip.To4() == nil
		}) || len(parts) == 2 && bytes.Compare(net.ParseIP(parts[0]).To4(), net.ParseIP(parts[1]).To4()) > 0 {
			return fmt.Errorf("azure user delegation signed_ip must be an IPv4 address or range")
		}
	}
	return nil
}

func storageTierRank(tier StorageCredentialTier) int {
	switch tier {
	case StorageRead:
		return 1
	case StorageAppend:
		return 2
	case StorageMaintain:
		return 3
	case StorageLock:
		return 4
	default:
		return 0
	}
}

func validServiceVersion(value string) bool {
	_, err := time.Parse("2006-01-02", value)
	return err == nil && value >= "2020-12-06" && value <= "2026-04-06"
}

func validateStaticCredentialPolicy(policy StaticCredentialPolicy, references map[string]struct{}) error {
	if policy.Generation == 0 {
		return fmt.Errorf("static credential policy requires a non-zero generation")
	}
	for index, generation := range policy.RevokedGenerations {
		if generation == 0 || generation >= policy.Generation ||
			index > 0 && policy.RevokedGenerations[index-1] >= generation {
			return fmt.Errorf("revoked static credential generations must be non-zero, older, unique, and ordered")
		}
	}
	found, err := addCredentialBindings(policy.Bindings, references)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("remote endpoint requires at least one static credential binding")
	}
	return nil
}

func validateSTSPolicy(policy S3STSPolicy, storageEndpoint map[string]any) error {
	if !validReference(policy.IssuerRef) || policy.Endpoint == "" || policy.Region == "" || policy.SessionName == "" {
		return fmt.Errorf("STS credential policy is incomplete")
	}
	if policy.Fallback != STSNoFallback && policy.Fallback != STSStaticOnUnavailable {
		return fmt.Errorf("invalid STS fallback policy %q", policy.Fallback)
	}
	for _, roleARN := range policy.Roles.references() {
		if roleARN != "" && !strings.HasPrefix(roleARN, "arn:") {
			return fmt.Errorf("STS role binding requires a role ARN")
		}
	}
	if !slices.ContainsFunc(policy.Roles.references(), func(role string) bool { return role != "" }) {
		return fmt.Errorf("STS credential policy requires at least one role binding")
	}
	parsed, err := url.Parse(policy.Endpoint)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" ||
		(parsed.Path != "" && parsed.Path != "/") {
		return fmt.Errorf("STS endpoint has invalid URL")
	}
	storageURL, _ := storageEndpoint["url"].(string)
	storageParsed, _ := url.Parse(storageURL)
	_, profile, normalizeErr := s3.NormalizeConfig(s3.Config{
		Endpoint: storageParsed.Host, UseHTTP: storageParsed.Scheme == "http",
		Bucket: storageEndpoint["bucket"].(string), Region: storageEndpoint["region"].(string),
		Provider: optionalString(storageEndpoint, "provider"), BucketLookup: optionalString(storageEndpoint, "bucket_lookup"),
	})
	if normalizeErr != nil {
		return fmt.Errorf("S3 endpoint: %w", normalizeErr)
	}
	if profile.Provider == s3.ProviderBackblaze {
		return fmt.Errorf("backblaze S3 does not support STS")
	}
	if profile.Provider == s3.ProviderWasabi && policy.Endpoint != "https://sts.wasabisys.com" {
		return fmt.Errorf("wasabi STS endpoint must be https://sts.wasabisys.com")
	}
	if parsed.Scheme != "https" && (parsed.Scheme != "http" || !strings.HasPrefix(storageURL, "http://")) {
		return fmt.Errorf("STS endpoint requires HTTPS unless the S3 endpoint explicitly uses HTTP")
	}
	return nil
}

func addCredentialBindings(bindings CredentialBindings, references map[string]struct{}) (bool, error) {
	found := false
	for _, reference := range bindings.references() {
		if reference == "" {
			continue
		}
		if !validReference(reference) {
			return false, fmt.Errorf("remote endpoint contains an invalid credential binding")
		}
		references[reference] = struct{}{}
		found = true
	}
	return found, nil
}

func validReference(reference string) bool {
	name, ok := strings.CutPrefix(reference, "cred:")
	if !ok || name == "" {
		return false
	}
	for _, character := range name {
		if !validReferenceCharacter(character) {
			return false
		}
	}
	return true
}

func validReferenceCharacter(character rune) bool {
	return character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' ||
		character >= '0' && character <= '9' || strings.ContainsRune("-_.", character)
}

func validateEndpoint(provider Provider, endpoint map[string]any) error {
	requiredByProvider := map[Provider][]string{
		ProviderLocal:       {"data_dir"},
		ProviderS3:          {"url", "bucket", "region"},
		ProviderAzure:       {"url", "account", "container"},
		ProviderGCS:         {"bucket"},
		ProviderGoogleDrive: {"drive_id", "root_folder_id", "path"},
	}
	allowedByProvider := map[Provider]map[string]struct{}{
		ProviderLocal:       {"data_dir": {}},
		ProviderS3:          {"url": {}, "bucket": {}, "prefix": {}, "region": {}, "provider": {}, "bucket_lookup": {}, "storage_class": {}, "tls_sha256": {}},
		ProviderAzure:       {"url": {}, "account": {}, "container": {}, "prefix": {}, "tls_sha256": {}},
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
	if provider == ProviderS3 {
		if err := validateS3Endpoint(endpoint); err != nil {
			return err
		}
	}
	if provider == ProviderAzure {
		if err := validateAzureEndpoint(endpoint); err != nil {
			return err
		}
	}
	return nil
}

func validateAzureEndpoint(endpoint map[string]any) error {
	parsed, err := url.Parse(endpoint["url"].(string))
	validPath := parsed != nil && (parsed.Path == "" || parsed.Path == "/")
	loopbackHTTP := parsed != nil && parsed.Scheme == "http" &&
		(parsed.Hostname() == "127.0.0.1" || parsed.Hostname() == "::1" || parsed.Hostname() == "localhost")
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" ||
		!validPath || parsed.Scheme != "https" && !loopbackHTTP {
		return fmt.Errorf("azure endpoint has invalid url")
	}
	if account, suffix, found := strings.Cut(parsed.Hostname(), ".blob."); found &&
		(account == "" || suffix == "" || account != endpoint["account"].(string)) {
		return fmt.Errorf("azure account does not match endpoint host")
	}
	return nil
}

func validateS3Endpoint(endpoint map[string]any) error {
	rawURL := endpoint["url"].(string)
	parsed, err := url.Parse(rawURL)
	validPath := parsed != nil && (parsed.Path == "" || parsed.Path == "/")
	validScheme := parsed != nil && (parsed.Scheme == "https" || parsed.Scheme == "http")
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || !validPath || !validScheme {
		return fmt.Errorf("s3 endpoint has invalid url")
	}
	_, _, err = s3.NormalizeConfig(s3.Config{
		Endpoint: parsed.Host, UseHTTP: parsed.Scheme == "http",
		Bucket: endpoint["bucket"].(string), Region: endpoint["region"].(string),
		Provider: optionalString(endpoint, "provider"), BucketLookup: optionalString(endpoint, "bucket_lookup"),
		StorageClass: optionalString(endpoint, "storage_class"),
	})
	if err != nil {
		return fmt.Errorf("s3 endpoint: %w", err)
	}
	return nil
}

func optionalString(values map[string]any, field string) string {
	value, _ := values[field].(string)
	return value
}
