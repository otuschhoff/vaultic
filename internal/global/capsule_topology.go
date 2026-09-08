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
	var primaryURL string
	for _, declared := range document.PackBackends {
		credential, credentialErr := leaseTopologyCredential(ctx, client, globalOptions, declared.CredentialRef)
		if credentialErr != nil {
			return capsuleTopologyBackends{}, credentialErr
		}
		openedBackend, description, openErr := openStructuredBackend(ctx, globalOptions, printer, declared, credential)
		credential = topology.Credential{}
		if openErr != nil {
			return capsuleTopologyBackends{}, fmt.Errorf("open capsule backend %q: %w", declared.ID, openErr)
		}
		opened = append(opened, openedBackend)
		placements[repository.PlacementBackendHash(declared.ID)] = openedBackend
		if primary == nil && declared.Role == topology.RolePrimary {
			primary, primaryURL = openedBackend, description
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
	primary, primaryURL, err = openStructuredBackend(ctx, globalOptions, printer, primaryDeclaration, credential)
	credential = topology.Credential{}
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

func openStructuredBackend(ctx context.Context, globalOptions Options, printer vaultic.Printer, declared topology.PackBackend, credential topology.Credential) (backend.Backend, string, error) {
	endpoint := func(name string) (string, error) {
		value, ok := declared.Endpoint[name].(string)
		if !ok || value == "" {
			return "", fmt.Errorf("backend %q endpoint %q is missing", declared.ID, name)
		}
		return value, nil
	}
	var scheme, description string
	var config any
	switch declared.Provider {
	case topology.ProviderLocal:
		location, err := endpoint("data_dir")
		if err != nil {
			return nil, "", err
		}
		opened, err := innerOpenBackend(ctx, location, globalOptions, globalOptions.Extended, false, printer)
		return opened, location, err
	case topology.ProviderS3:
		rawEndpoint, err := endpoint("url")
		if err != nil {
			return nil, "", err
		}
		parsed, err := url.Parse(rawEndpoint)
		if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Path != "" && parsed.Path != "/" || parsed.Scheme != "https" && parsed.Scheme != "http" {
			return nil, "", fmt.Errorf("backend %q has invalid S3 endpoint", declared.ID)
		}
		bucket, err := endpoint("bucket")
		if err != nil {
			return nil, "", err
		}
		cfg := s3.NewConfig()
		cfg.Endpoint, cfg.UseHTTP, cfg.Bucket = parsed.Host, parsed.Scheme == "http", bucket
		cfg.Prefix = optionalEndpointString(declared.Endpoint, "prefix")
		cfg.Region = optionalEndpointString(declared.Endpoint, "region")
		cfg.StorageClass = optionalEndpointString(declared.Endpoint, "storage_class")
		if credential.Kind != topology.CredentialNone {
			cfg.KeyID = credential.AccessKeyID
			cfg.Secret = options.NewSecretString(credential.SecretAccessKey)
		}
		scheme, config, description = "s3", &cfg, fmt.Sprintf("s3:%s/%s/%s", cfg.Endpoint, cfg.Bucket, cfg.Prefix)
	case topology.ProviderAzure:
		rawEndpoint, err := endpoint("url")
		if err != nil {
			return nil, "", err
		}
		parsed, err := url.Parse(rawEndpoint)
		if err != nil || parsed.Scheme != "https" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Path != "" && parsed.Path != "/" {
			return nil, "", fmt.Errorf("backend %q has invalid Azure endpoint", declared.ID)
		}
		hostParts := strings.SplitN(parsed.Hostname(), ".blob.", 2)
		if len(hostParts) != 2 || hostParts[0] == "" || hostParts[1] == "" {
			return nil, "", fmt.Errorf("backend %q Azure endpoint must be account.blob.SUFFIX", declared.ID)
		}
		container, err := endpoint("container")
		if err != nil {
			return nil, "", err
		}
		cfg := azure.NewConfig()
		cfg.Container = container
		cfg.EndpointSuffix = hostParts[1]
		cfg.Prefix = optionalEndpointString(declared.Endpoint, "prefix")
		cfg.AccountName = hostParts[0]
		if credential.AccountName != "" && credential.AccountName != cfg.AccountName {
			return nil, "", fmt.Errorf("backend %q Azure account does not match its endpoint", declared.ID)
		}
		cfg.AccountKey = options.NewSecretString(credential.AccountKey)
		cfg.AccountSAS = options.NewSecretString(credential.SASToken)
		scheme, config, description = "azure", &cfg, "azure:"+container+":"+cfg.Prefix
	case topology.ProviderGCS:
		bucket, err := endpoint("bucket")
		if err != nil {
			return nil, "", err
		}
		cfg := gs.NewConfig()
		cfg.Bucket = bucket
		cfg.Prefix = optionalEndpointString(declared.Endpoint, "prefix")
		if credential.Kind == topology.CredentialNone {
			ctx = gs.WithWorkloadIdentity(ctx)
		} else {
			ctx = gs.WithServiceAccountCredential(ctx, gs.ServiceAccountCredential{JSON: []byte(credential.ServiceAccountJSON), Subject: credential.Subject})
		}
		scheme, config, description = "gs", &cfg, "gs:"+bucket+":"+cfg.Prefix
	case topology.ProviderGoogleDrive:
		driveID, err := endpoint("drive_id")
		if err != nil {
			return nil, "", err
		}
		cfg := gdrive.NewConfig()
		cfg.DriveID = driveID
		cfg.RootFolderID = optionalEndpointString(declared.Endpoint, "root_folder_id")
		cfg.Prefix = optionalEndpointString(declared.Endpoint, "path")
		ctx = gdrive.WithCredentials(ctx, gdrive.Credentials{ClientID: credential.ClientID, ClientSecret: credential.ClientSecret, RefreshToken: credential.RefreshToken, TokenURI: credential.TokenURI, Scopes: credential.Scopes, ServiceAccountJSON: []byte(credential.ServiceAccountJSON), Subject: credential.Subject})
		scheme, config, description = "gdrive", &cfg, "gdrive:"+driveID+"/"+cfg.Prefix
	default:
		return nil, "", fmt.Errorf("unsupported capsule backend provider %q", declared.Provider)
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
	opened, err := createOrOpenBackend(ctx, backendOpenRequest{scheme: scheme, config: config, transport: roundTripper, limiter: limiter, globalOptions: globalOptions, repository: description, create: false, printer: printer})
	if err != nil {
		return nil, "", err
	}
	opened, err = wrapBackend(opened, globalOptions, printer)
	return opened, description, err
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
