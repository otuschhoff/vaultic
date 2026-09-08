package global

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
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
}

func openCapsuleTopology(ctx context.Context, globalOptions Options, printer vaultic.Printer) (_ capsuleTopologyBackends, err error) {
	if globalOptions.KeyBrokerSocket == "" || globalOptions.KeyBrokerReleaseManifest == "" {
		return capsuleTopologyBackends{}, fmt.Errorf("capsule topology requires key broker socket and release manifest")
	}
	client, err := indexbroker.Dial(ctx, globalOptions.KeyBrokerSocket)
	if err != nil {
		return capsuleTopologyBackends{}, err
	}
	defer func() {
		if err != nil {
			_ = client.Close() // Preserve the topology-open error; connection cleanup is best effort.
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
		credential, credentialErr := leaseTopologyCredential(ctx, client, globalOptions, declared.CredentialRef)
		if credentialErr != nil {
			return capsuleTopologyBackends{}, credentialErr
		}
		openedBackend, _, openErr := openStructuredBackend(ctx, globalOptions, printer, declared, credential)
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
	credential, err := leaseTopologyCredential(ctx, client, globalOptions, primaryDeclaration.CredentialRef)
	if err != nil {
		return capsuleTopologyBackends{}, err
	}
	primary, primaryURL, err := openStructuredBackend(ctx, globalOptions, printer, primaryDeclaration, credential)
	if err != nil {
		return capsuleTopologyBackends{}, err
	}
	opened = append(opened, primary)
	return capsuleTopologyBackends{document: document, primary: primary, primaryURL: primaryURL, placements: placements, client: client}, nil
}

func leaseTopologyCredential(ctx context.Context, client *indexbroker.Client, globalOptions Options, reference string) (topology.Credential, error) {
	if reference == "" {
		return topology.Credential{Kind: topology.CredentialNone}, nil
	}
	lease, err := client.AcquireCredentialLease(ctx, globalOptions.KeyBrokerReleaseManifest, reference, globalOptions.KeyBrokerLeaseDuration)
	if err != nil {
		return topology.Credential{}, err
	}
	defer clear(lease.Key)
	if lease.ExpiresUnixMS <= uint64(time.Now().UnixMilli()) {
		return topology.Credential{}, fmt.Errorf("key broker returned an expired credential lease")
	}
	var credential topology.Credential
	if err := json.Unmarshal(lease.Key, &credential); err != nil {
		return topology.Credential{}, fmt.Errorf("decode credential lease %q: %w", reference, err)
	}
	if err := credential.Validate(); err != nil {
		return topology.Credential{}, fmt.Errorf("validate credential lease %q: %w", reference, err)
	}
	return credential, nil
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
	config.StorageClass = optionalEndpointString(declared.Endpoint, "storage_class")
	if credential.Kind != topology.CredentialNone {
		config.KeyID = credential.AccessKeyID
		config.Secret = options.NewSecretString(credential.SecretAccessKey)
	}
	return &config, fmt.Sprintf("s3:%s/%s/%s", config.Endpoint, config.Bucket, config.Prefix), nil
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
	container, err := requiredEndpointString(declared, "container")
	if err != nil {
		return nil, "", err
	}
	config := azure.NewConfig()
	config.Container = container
	config.EndpointSuffix = hostParts[1]
	config.Prefix = optionalEndpointString(declared.Endpoint, "prefix")
	config.AccountName = hostParts[0]
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
	if credential.Kind == topology.CredentialNone {
		ctx = gs.WithWorkloadIdentity(ctx)
	} else {
		ctx = gs.WithServiceAccountCredential(ctx, gs.ServiceAccountCredential{
			JSON: []byte(credential.ServiceAccountJSON), Subject: credential.Subject,
		})
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
