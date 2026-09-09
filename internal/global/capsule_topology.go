package global

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/backend/azure"
	"github.com/otuschhoff/vaultic/internal/backend/gdrive"
	"github.com/otuschhoff/vaultic/internal/backend/gs"
	"github.com/otuschhoff/vaultic/internal/backend/s3"
	indexbroker "github.com/otuschhoff/vaultic/internal/index/broker"
	"github.com/otuschhoff/vaultic/internal/observability"
	"github.com/otuschhoff/vaultic/internal/options"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/topology"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

type capsuleTopologyBackends struct {
	document   topology.Document
	primary    backend.Backend
	primaryURL string
	placements map[uint64]backend.Backend
	client     *indexbroker.Client
	manager    *storageCredentialManager
}

func storageCredentialLifetime(globalOptions Options) (time.Duration, time.Duration, time.Duration, error) {
	ttl := globalOptions.StorageTokenTTL
	if ttl == 0 {
		ttl = time.Hour
	}
	margin := globalOptions.StorageTokenRenewMargin
	if margin == 0 {
		margin = max(20*time.Minute, ttl/3)
	}
	grace := globalOptions.BrokerOutageGrace
	if grace == 0 {
		grace = ttl
	}
	if ttl <= 0 || ttl > time.Hour || ttl%time.Second != 0 {
		return 0, 0, 0, fmt.Errorf("storage token TTL must be positive whole seconds and at most one hour")
	}
	if margin <= 0 || margin >= ttl {
		return 0, 0, 0, fmt.Errorf("storage token renewal margin must be positive and less than the token TTL")
	}
	if grace <= 0 {
		return 0, 0, 0, fmt.Errorf("broker outage grace must be positive")
	}
	return ttl, margin, grace, nil
}

func openCapsuleTopology(ctx context.Context, globalOptions Options, printer vaultic.Printer) (_ capsuleTopologyBackends, err error) {
	if globalOptions.KeyBrokerSocket == "" || globalOptions.KeyBrokerReleaseManifest == "" {
		return capsuleTopologyBackends{}, fmt.Errorf("capsule topology requires key broker socket and release manifest")
	}
	storageTTL, _, _, err := storageCredentialLifetime(globalOptions)
	if err != nil {
		return capsuleTopologyBackends{}, err
	}
	globalOptions.StorageTokenTTL = storageTTL
	client, err := indexbroker.Dial(ctx, globalOptions.KeyBrokerSocket)
	if err != nil {
		return capsuleTopologyBackends{}, err
	}
	defer func() {
		if err != nil {
			_ = client.Close() // Preserve the topology-open error; connection cleanup is best effort.
		}
	}()
	manager, err := newStorageCredentialManager(client, globalOptions, printer)
	if err != nil {
		return capsuleTopologyBackends{}, err
	}
	defer func() {
		if err != nil {
			_ = manager.Close() // Preserve the topology-open error; manager cleanup is best effort.
		}
	}()
	document, topologyLease, err := client.ReadTopology(ctx, globalOptions.KeyBrokerReleaseManifest, globalOptions.KeyBrokerLeaseDuration)
	if err != nil {
		return capsuleTopologyBackends{}, err
	}
	clear(topologyLease.Key)
	if err := applyTopologyOverrides(&document, globalOptions.TopologyOverrides); err != nil {
		return capsuleTopologyBackends{}, err
	}
	storageTier := globalOptions.StorageCredentialTier
	if storageTier == "" {
		storageTier = string(topology.StorageMaintain)
	}
	placements := make(map[uint64]backend.Backend, len(document.PackBackends))
	opened := make([]backend.Backend, 0, len(document.PackBackends)+1)
	defer func() {
		if err != nil {
			for _, item := range opened {
				_ = item.Close() // Preserve the topology-open error; backend cleanup is best effort.
			}
		}
	}()
	var primary backend.Backend
	for _, declared := range document.PackBackends {
		openedBackend, _, openErr := openCapsulePackBackend(
			ctx, manager, globalOptions, printer, declared, storageTier,
		)
		if openErr != nil {
			return capsuleTopologyBackends{}, fmt.Errorf("open capsule backend %q: %w", declared.ID, openErr)
		}
		opened = append(opened, openedBackend)
		placements[repository.PlacementBackendHash(declared.ID)] = openedBackend
		if primary == nil && declared.Role == topology.RolePrimary {
			primary = openedBackend
		}
	}
	if primary == nil {
		return capsuleTopologyBackends{}, fmt.Errorf("capsule topology has no primary pack backend")
	}
	// Open a distinct primary handle because Repository owns its primary and placement handles independently.
	primaryDeclaration := document.PackBackends[0]
	for _, declared := range document.PackBackends {
		if declared.Role == topology.RolePrimary {
			primaryDeclaration = declared
			break
		}
	}
	primary, primaryURL, err := openCapsulePrimaryBackend(
		ctx, manager, globalOptions, printer, primaryDeclaration, storageTier,
	)
	if err != nil {
		return capsuleTopologyBackends{}, err
	}
	opened = append(opened, primary)
	manager.start()
	managedPrimary := &credentialManagedBackend{Backend: primary, manager: manager}
	return capsuleTopologyBackends{document: document, primary: managedPrimary, primaryURL: primaryURL, placements: placements, client: client, manager: manager}, nil
}

type credentialManagedBackend struct {
	backend.Backend
	manager *storageCredentialManager
}

func (b *credentialManagedBackend) Close() error {
	return errors.Join(b.manager.Close(), b.Backend.Close())
}

func (b *credentialManagedBackend) Unwrap() backend.Backend { return b.Backend }

func openCapsulePackBackend(
	ctx context.Context,
	manager *storageCredentialManager,
	globalOptions Options,
	printer vaultic.Printer,
	declared topology.PackBackend,
	storageTier string,
) (backend.Backend, string, error) {
	if declared.Provider == topology.ProviderLocal {
		return openStructuredBackend(ctx, globalOptions, printer, declared, topology.Credential{Kind: topology.CredentialNone})
	}
	credential, err := leaseTopologyStorageCredential(
		ctx, manager.client, globalOptions, "pack:"+declared.ID, storageTier,
	)
	if err != nil {
		return nil, "", err
	}
	opened, description, err := openStructuredBackend(ctx, globalOptions, printer, declared, credential.credential)
	if err != nil {
		_ = manager.client.ReleaseLease(ctx, credential.leaseID) // Preserve the backend-open error; lease cleanup is best effort.
		return nil, "", err
	}
	validUntil := credential.expiresAt
	if graceUntil := time.Now().Add(manager.outageGrace); graceUntil.Before(validUntil) {
		validUntil = graceUntil
	}
	renewable := newRenewableBackend(opened, validUntil, manager.options.StorageTokenTTL)
	manager.add(&storageCredentialSlot{
		backend: renewable, declared: declared, target: "pack:" + declared.ID,
		tier: storageTier, leaseID: credential.leaseID, expiresAt: credential.expiresAt,
	})
	return renewable, description, nil
}

func openCapsulePrimaryBackend(
	ctx context.Context,
	manager *storageCredentialManager,
	globalOptions Options,
	printer vaultic.Printer,
	declared topology.PackBackend,
	storageTier string,
) (backend.Backend, string, error) {
	dataBackend, description, err := openCapsulePackBackend(
		ctx, manager, globalOptions, printer, declared, storageTier,
	)
	if err != nil || !globalOptions.StorageLockCredential || declared.Provider == topology.ProviderLocal {
		return dataBackend, description, err
	}
	lockBackend, _, err := openCapsulePackBackend(
		ctx, manager, globalOptions, printer, declared, string(topology.StorageLock),
	)
	if err != nil {
		_ = dataBackend.Close() // Preserve the lock-backend error; data backend cleanup is best effort.
		return nil, "", fmt.Errorf("open lock backend %q: %w", declared.ID, err)
	}
	return &credentialRoutingBackend{data: dataBackend, lock: lockBackend}, description, nil
}

type credentialRoutingBackend struct {
	data backend.Backend
	lock backend.Backend
}

func (r *credentialRoutingBackend) route(fileType backend.FileType) backend.Backend {
	if fileType == backend.LockFile {
		return r.lock
	}
	return r.data
}

func (r *credentialRoutingBackend) Properties() backend.Properties { return r.data.Properties() }
func (r *credentialRoutingBackend) Hasher() hash.Hash              { return r.data.Hasher() }
func (r *credentialRoutingBackend) Unwrap() backend.Backend        { return r.data }
func (r *credentialRoutingBackend) Remove(ctx context.Context, handle backend.Handle) error {
	return r.route(handle.Type).Remove(ctx, handle)
}
func (r *credentialRoutingBackend) Close() error {
	return errors.Join(r.data.Close(), r.lock.Close())
}
func (r *credentialRoutingBackend) Save(ctx context.Context, handle backend.Handle, reader backend.RewindReader) error {
	return r.route(handle.Type).Save(ctx, handle, reader)
}
func (r *credentialRoutingBackend) Load(
	ctx context.Context,
	handle backend.Handle,
	length int,
	offset int64,
	fn func(io.Reader) error,
) error {
	return r.route(handle.Type).Load(ctx, handle, length, offset, fn)
}
func (r *credentialRoutingBackend) Stat(ctx context.Context, handle backend.Handle) (backend.FileInfo, error) {
	return r.route(handle.Type).Stat(ctx, handle)
}
func (r *credentialRoutingBackend) List(ctx context.Context, fileType backend.FileType, fn func(backend.FileInfo) error) error {
	return r.route(fileType).List(ctx, fileType, fn)
}
func (r *credentialRoutingBackend) IsNotExist(err error) bool { return r.data.IsNotExist(err) }
func (r *credentialRoutingBackend) IsPermanentError(err error) bool {
	return r.data.IsPermanentError(err)
}
func (r *credentialRoutingBackend) Delete(ctx context.Context) error { return r.data.Delete(ctx) }
func (r *credentialRoutingBackend) Warmup(ctx context.Context, handles []backend.Handle) ([]backend.Handle, error) {
	return r.data.Warmup(ctx, handles)
}
func (r *credentialRoutingBackend) WarmupWait(ctx context.Context, handles []backend.Handle) error {
	return r.data.WarmupWait(ctx, handles)
}

type leasedStorageCredential struct {
	credential topology.Credential
	leaseID    string
	expiresAt  time.Time
	source     string
}

func leaseTopologyStorageCredential(
	ctx context.Context,
	client *indexbroker.Client,
	globalOptions Options,
	storageTarget, storageTier string,
) (_ leasedStorageCredential, err error) {
	lease, err := client.AcquireStorageCredentialLease(
		ctx,
		globalOptions.KeyBrokerReleaseManifest,
		storageTarget,
		storageTier,
		globalOptions.StorageTokenTTL,
	)
	if err != nil {
		return leasedStorageCredential{}, err
	}
	defer clear(lease.Key)
	defer func() {
		if err != nil {
			_ = client.ReleaseLease(context.Background(), lease.LeaseID) // Preserve validation errors; lease cleanup is best effort.
		}
	}()
	if lease.ExpiresUnixMS <= uint64(time.Now().UnixMilli()) {
		return leasedStorageCredential{}, fmt.Errorf("key broker returned an expired storage credential lease")
	}
	if lease.StorageTarget != storageTarget || lease.StorageTier != storageTier ||
		lease.CredentialSource != "sts" && lease.CredentialSource != "azure-user-delegation" && lease.CredentialSource != "gcp-downscope" && lease.CredentialSource != "static" &&
			lease.CredentialSource != "static-fallback" {
		return leasedStorageCredential{}, fmt.Errorf("key broker returned mismatched storage credential metadata")
	}
	if (lease.CredentialSource == "static" || lease.CredentialSource == "static-fallback") && lease.StaticGeneration == 0 {
		return leasedStorageCredential{}, fmt.Errorf("key broker returned static credentials without a generation")
	}
	if (lease.CredentialSource == "sts" || lease.CredentialSource == "azure-user-delegation" || lease.CredentialSource == "gcp-downscope") && lease.ProviderExpiresAt == "" {
		return leasedStorageCredential{}, fmt.Errorf("key broker returned dynamic credentials without a provider expiry")
	}
	var credential topology.Credential
	if err := json.Unmarshal(lease.Key, &credential); err != nil {
		return leasedStorageCredential{}, fmt.Errorf("decode storage credential lease: %w", err)
	}
	if err := credential.Validate(); err != nil {
		return leasedStorageCredential{}, fmt.Errorf("validate storage credential lease: %w", err)
	}
	expiresAt := time.UnixMilli(int64(lease.ExpiresUnixMS))
	if lease.ProviderExpiresAt != "" {
		providerExpiry, parseErr := time.Parse(time.RFC3339, lease.ProviderExpiresAt)
		if parseErr != nil {
			return leasedStorageCredential{}, fmt.Errorf("parse provider storage credential expiry: %w", parseErr)
		}
		if providerExpiry.Before(expiresAt) {
			expiresAt = providerExpiry
		}
	}
	leased := leasedStorageCredential{credential: credential, leaseID: lease.LeaseID, expiresAt: expiresAt, source: lease.CredentialSource}
	observability.EmitBestEffort(ctx, observability.Event{
		Severity: observability.Notice, Category: observability.CategoryAuth, Component: "storage-credential-manager",
		Message: "storage credential granted", Fields: map[string]any{
			"storage_target": storageTarget, "storage_tier": storageTier,
			"ttl_seconds": int64(time.Until(expiresAt).Seconds()), "lease_id": lease.LeaseID,
			"credential_source": lease.CredentialSource,
		},
	})
	return leased, nil
}

func openStructuredBackend(
	ctx context.Context,
	globalOptions Options,
	printer vaultic.Printer,
	declared topology.PackBackend,
	credential topology.Credential,
) (backend.Backend, string, error) {
	if declared.Provider == topology.ProviderLocal {
		location, err := requiredEndpointString(declared, "data_dir")
		if err != nil {
			return nil, "", err
		}
		opened, err := innerOpenBackend(ctx, location, globalOptions, globalOptions.Extended, false, printer)
		return opened, location, err
	}
	ctx, scheme, config, description, err := structuredBackendConfig(ctx, declared, credential)
	if err != nil {
		return nil, "", err
	}
	roundTripper, limiter, err := setupTransport(globalOptions)
	if err != nil {
		return nil, "", err
	}
	if pin, _ := declared.Endpoint["tls_sha256"].(string); pin != "" {
		expected, decodeErr := hex.DecodeString(pin)
		if decodeErr != nil || len(expected) != sha256.Size {
			return nil, "", fmt.Errorf("backend %q has invalid tls_sha256", declared.ID)
		}
		roundTripper = pinnedRoundTripper{base: roundTripper, expected: expected}
	}
	opened, err := createOrOpenBackend(ctx, backendOpenRequest{
		scheme: scheme, config: config, transport: roundTripper, limiter: limiter,
		globalOptions: globalOptions, repository: description, create: false, printer: printer,
	})
	if err != nil {
		return nil, "", err
	}
	opened, err = wrapBackend(opened, globalOptions, printer)
	return opened, description, err
}

func requiredEndpointString(declared topology.PackBackend, name string) (string, error) {
	value, ok := declared.Endpoint[name].(string)
	if !ok || value == "" {
		return "", fmt.Errorf("backend %q endpoint %q is missing", declared.ID, name)
	}
	return value, nil
}

func structuredBackendConfig(
	ctx context.Context,
	declared topology.PackBackend,
	credential topology.Credential,
) (context.Context, string, any, string, error) {
	switch declared.Provider {
	case topology.ProviderS3:
		config, description, err := s3BackendConfig(declared, credential)
		return ctx, "s3", config, description, err
	case topology.ProviderAzure:
		config, description, err := azureBackendConfig(declared, credential)
		return ctx, "azure", config, description, err
	case topology.ProviderGCS:
		config, description, configuredCtx, err := gcsBackendConfig(ctx, declared, credential)
		return configuredCtx, "gs", config, description, err
	case topology.ProviderGoogleDrive:
		config, description, configuredCtx, err := gdriveBackendConfig(ctx, declared, credential)
		return configuredCtx, "gdrive", config, description, err
	default:
		return ctx, "", nil, "", fmt.Errorf("unsupported capsule backend provider %q", declared.Provider)
	}
}

func s3BackendConfig(declared topology.PackBackend, credential topology.Credential) (*s3.Config, string, error) {
	rawEndpoint, err := requiredEndpointString(declared, "url")
	if err != nil {
		return nil, "", err
	}
	parsed, err := url.Parse(rawEndpoint)
	validPath := parsed != nil && (parsed.Path == "" || parsed.Path == "/")
	validScheme := parsed != nil && (parsed.Scheme == "https" || parsed.Scheme == "http")
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || !validPath || !validScheme {
		return nil, "", fmt.Errorf("backend %q has invalid S3 endpoint", declared.ID)
	}
	bucket, err := requiredEndpointString(declared, "bucket")
	if err != nil {
		return nil, "", err
	}
	config := s3.NewConfig()
	config.Endpoint, config.UseHTTP, config.Bucket = parsed.Host, parsed.Scheme == "http", bucket
	config.Prefix = optionalEndpointString(declared.Endpoint, "prefix")
	config.Region = optionalEndpointString(declared.Endpoint, "region")
	config.Provider = optionalEndpointString(declared.Endpoint, "provider")
	config.BucketLookup = optionalEndpointString(declared.Endpoint, "bucket_lookup")
	config.StorageClass = optionalEndpointString(declared.Endpoint, "storage_class")
	normalized, _, err := s3.NormalizeConfig(config)
	if err != nil {
		return nil, "", fmt.Errorf("backend %q: %w", declared.ID, err)
	}
	config = normalized
	if credential.Kind != topology.CredentialNone {
		config.KeyID = credential.AccessKeyID
		config.Secret = options.NewSecretString(credential.SecretAccessKey)
		config.SessionToken = credential.SessionToken
		config.BrokerCredential = true
	}
	prefixDigest := sha256.Sum256([]byte(config.Prefix))
	return &config, fmt.Sprintf("s3:%s/%s/<prefix-sha256:%x>", config.Endpoint, config.Bucket, prefixDigest), nil
}

func azureBackendConfig(declared topology.PackBackend, credential topology.Credential) (*azure.Config, string, error) {
	rawEndpoint, err := requiredEndpointString(declared, "url")
	if err != nil {
		return nil, "", err
	}
	parsed, err := url.Parse(rawEndpoint)
	validPath := parsed != nil && (parsed.Path == "" || parsed.Path == "/")
	if err != nil || parsed.Scheme != "https" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || !validPath {
		return nil, "", fmt.Errorf("backend %q has invalid Azure endpoint", declared.ID)
	}
	hostParts := strings.SplitN(parsed.Hostname(), ".blob.", 2)
	if len(hostParts) != 2 || hostParts[0] == "" || hostParts[1] == "" {
		return nil, "", fmt.Errorf("backend %q Azure endpoint must be account.blob.SUFFIX", declared.ID)
	}
	account, err := requiredEndpointString(declared, "account")
	if err != nil {
		return nil, "", err
	}
	if account != hostParts[0] {
		return nil, "", fmt.Errorf("backend %q Azure account does not match its endpoint", declared.ID)
	}
	container, err := requiredEndpointString(declared, "container")
	if err != nil {
		return nil, "", err
	}
	config := azure.NewConfig()
	config.Container = container
	config.EndpointSuffix = hostParts[1]
	config.Prefix = optionalEndpointString(declared.Endpoint, "prefix")
	config.AccountName = account
	if credential.AccountName != "" && credential.AccountName != config.AccountName {
		return nil, "", fmt.Errorf("backend %q Azure account does not match its endpoint", declared.ID)
	}
	config.AccountKey = options.NewSecretString(credential.AccountKey)
	config.AccountSAS = options.NewSecretString(credential.SASToken)
	return &config, "azure:" + container + ":" + config.Prefix, nil
}

func gcsBackendConfig(
	ctx context.Context,
	declared topology.PackBackend,
	credential topology.Credential,
) (*gs.Config, string, context.Context, error) {
	bucket, err := requiredEndpointString(declared, "bucket")
	if err != nil {
		return nil, "", ctx, err
	}
	config := gs.NewConfig()
	config.Bucket = bucket
	config.Prefix = optionalEndpointString(declared.Endpoint, "prefix")
	switch credential.Kind {
	case topology.CredentialNone:
		ctx = gs.WithWorkloadIdentity(ctx)
	case topology.CredentialGCPServiceAccountJSON:
		ctx = gs.WithServiceAccountCredential(ctx, gs.ServiceAccountCredential{
			JSON: []byte(credential.ServiceAccountJSON), Subject: credential.Subject,
		})
	case topology.CredentialGCPAccessToken:
		expiresAt, parseErr := time.Parse(time.RFC3339, credential.ExpiresAt)
		if parseErr != nil {
			return nil, "", ctx, fmt.Errorf("backend %q GCP access token has invalid expiry: %w", declared.ID, parseErr)
		}
		ctx = gs.WithAccessTokenCredential(ctx, gs.AccessTokenCredential{
			Token: credential.AccessToken, Expiry: expiresAt,
		})
	default:
		return nil, "", ctx, fmt.Errorf("backend %q requires a GCS credential", declared.ID)
	}
	return &config, "gs:" + bucket + ":" + config.Prefix, ctx, nil
}

func gdriveBackendConfig(
	ctx context.Context,
	declared topology.PackBackend,
	credential topology.Credential,
) (*gdrive.Config, string, context.Context, error) {
	driveID, err := requiredEndpointString(declared, "drive_id")
	if err != nil {
		return nil, "", ctx, err
	}
	config := gdrive.NewConfig()
	config.DriveID = driveID
	config.RootFolderID = optionalEndpointString(declared.Endpoint, "root_folder_id")
	config.Prefix = optionalEndpointString(declared.Endpoint, "path")
	ctx = gdrive.WithCredentials(ctx, gdrive.Credentials{
		ClientID: credential.ClientID, ClientSecret: credential.ClientSecret,
		RefreshToken: credential.RefreshToken, TokenURI: credential.TokenURI,
		Scopes: credential.Scopes, ServiceAccountJSON: []byte(credential.ServiceAccountJSON), Subject: credential.Subject,
	})
	return &config, "gdrive:" + driveID + "/" + config.Prefix, ctx, nil
}

type pinnedRoundTripper struct {
	base     http.RoundTripper
	expected []byte
}

func (transport pinnedRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := transport.base.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	if response.TLS == nil || len(response.TLS.PeerCertificates) == 0 {
		_ = response.Body.Close() // Preserve the missing-TLS error; response cleanup is best effort.
		return nil, fmt.Errorf("TLS certificate pin requires an HTTPS connection")
	}
	actual := sha256.Sum256(response.TLS.PeerCertificates[0].Raw)
	if subtle.ConstantTimeCompare(actual[:], transport.expected) != 1 {
		_ = response.Body.Close() // Preserve the pin mismatch; response cleanup is best effort.
		return nil, fmt.Errorf("TLS certificate pin mismatch")
	}
	return response, nil
}

func validateTopologySource(value string, brokerConfigured bool) (string, error) {
	if value == "" {
		if brokerConfigured {
			return "capsule", nil
		}
		return "external", nil
	}
	if value != "capsule" && value != "external" {
		return "", fmt.Errorf("topology source must be capsule or external")
	}
	return value, nil
}

func optionalEndpointString(endpoint map[string]any, name string) string {
	value, ok := endpoint[name].(string)
	if !ok {
		return ""
	}
	return value
}

func applyTopologyOverrides(document *topology.Document, overrides []string) error {
	seen := make(map[string]struct{}, len(overrides))
	for _, override := range overrides {
		selector, value, ok := strings.Cut(override, "=")
		if !ok || value == "" {
			return fmt.Errorf("topology override must be ID.data_dir=PATH")
		}
		id, field, ok := strings.Cut(selector, ".")
		if !ok || id == "" || field != "data_dir" {
			return fmt.Errorf("only ID.data_dir topology overrides are supported")
		}
		if _, duplicate := seen[selector]; duplicate {
			return fmt.Errorf("duplicate topology override %q", selector)
		}
		seen[selector] = struct{}{}
		matched := false
		for index := range document.PackBackends {
			backend := &document.PackBackends[index]
			if backend.ID == id && backend.Provider == topology.ProviderLocal {
				backend.Endpoint["data_dir"] = value
				matched = true
			}
		}
		if !matched {
			return fmt.Errorf("topology override %q does not select a local pack backend", selector)
		}
	}
	return nil
}

func closeTopologyBackends(backends map[uint64]backend.Backend) {
	for _, item := range backends {
		_ = item.Close() // Repository opening already failed; backend cleanup is best effort.
	}
}
