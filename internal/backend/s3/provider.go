package s3

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
)

type Provider string

const (
	ProviderGeneric   Provider = "generic"
	ProviderBackblaze Provider = "backblaze"
	ProviderCeph      Provider = "ceph"
	ProviderWasabi    Provider = "wasabi"
)

type Capabilities struct {
	SignatureV4        bool
	MultipartUpload    bool
	RangeReads         bool
	ListObjectsV2      bool
	ConditionalCreate  string
	VersionRetention   string
	ObjectImmutability string
	STSRoleAssumption  string
	GlacierRestore     bool
}

type Profile struct {
	Provider     Provider
	Inferred     bool
	EndpointHost string
	Region       string
	BucketLookup string
	Capabilities Capabilities
}

type ProviderCompatibilityError struct {
	Provider Provider
	Feature  string
	Reason   string
}

type ErrorKind string

const (
	ErrorAuthentication ErrorKind = "authentication-denied"
	ErrorThrottling     ErrorKind = "throttling"
	ErrorQuota          ErrorKind = "quota-exceeded"
	ErrorRetention      ErrorKind = "retention-denied"
	ErrorUnsupportedAPI ErrorKind = "unsupported-api"
)

type ProviderError struct {
	Provider Provider
	Kind     ErrorKind
	Code     string
}

func (err *ProviderError) Error() string {
	return fmt.Sprintf("s3 provider %q %s (%s)", err.Provider, err.Kind, err.Code)
}

func (err *ProviderCompatibilityError) Error() string {
	return fmt.Sprintf("s3 provider %q does not support %s: %s", err.Provider, err.Feature, err.Reason)
}

func NormalizeConfig(cfg Config) (Config, Profile, error) {
	host := endpointHostname(cfg.Endpoint)
	inferredProvider, inferredRegion := inferProvider(host)
	provider, explicitSelection, explicitProvider, err := selectProvider(cfg.Provider, inferredProvider, host)
	if err != nil {
		return cfg, Profile{}, err
	}
	region, err := normalizeProviderRegion(cfg.Region, provider, inferredProvider, inferredRegion)
	if err != nil {
		return cfg, Profile{}, err
	}
	if err := validateNamedProvider(cfg, provider, inferredProvider, explicitProvider, host, region); err != nil {
		return cfg, Profile{}, err
	}
	bucketLookup, err := normalizeBucketLookup(cfg.BucketLookup, cfg.Bucket, provider)
	if err != nil {
		return cfg, Profile{}, err
	}

	cfg.Provider = string(provider)
	cfg.Region = region
	cfg.BucketLookup = bucketLookup
	return cfg, Profile{
		Provider: provider, Inferred: !explicitSelection && inferredProvider == provider,
		EndpointHost: host, Region: region, BucketLookup: bucketLookup,
		Capabilities: providerCapabilities(provider),
	}, nil
}

func selectProvider(raw string, inferred Provider, host string) (Provider, bool, bool, error) {
	rawProvider := strings.ToLower(strings.TrimSpace(raw))
	provider := Provider(rawProvider)
	explicitSelection := rawProvider != ""
	explicitProvider := explicitSelection && provider != ProviderGeneric
	if !explicitSelection {
		provider = inferred
	}
	if provider != ProviderGeneric && provider != ProviderBackblaze && provider != ProviderCeph && provider != ProviderWasabi {
		return "", false, false, fmt.Errorf("bad S3 provider %q: must be generic, backblaze, ceph, or wasabi", raw)
	}
	if explicitProvider && inferred != ProviderGeneric && inferred != provider {
		return "", false, false, fmt.Errorf("s3 provider %q does not match endpoint host %q", provider, host)
	}
	if explicitProvider && inferred == ProviderGeneric && providerDomainLookalike(host) {
		return "", false, false, fmt.Errorf("s3 provider %q endpoint host %q is not a valid provider endpoint", provider, host)
	}
	return provider, explicitSelection, explicitProvider, nil
}

func normalizeProviderRegion(raw string, provider, inferredProvider Provider, inferredRegion string) (string, error) {
	region := strings.TrimSpace(raw)
	if inferredProvider == provider && inferredRegion != "" {
		if region != "" && region != inferredRegion {
			return "", fmt.Errorf(
				"s3 provider %q endpoint region %q does not match configured region %q",
				provider, inferredRegion, region,
			)
		}
		if region == "" {
			region = inferredRegion
		}
	}
	return region, nil
}

func validateNamedProvider(
	cfg Config, provider, inferredProvider Provider, explicitProvider bool, host, region string,
) error {
	if provider == ProviderGeneric {
		return nil
	}
	if cfg.UseHTTP && inferredProvider == provider {
		return fmt.Errorf("s3 provider %q production endpoint requires HTTPS", provider)
	}
	if explicitProvider && inferredProvider == ProviderGeneric && region == "" {
		return fmt.Errorf("s3 provider %q with a custom endpoint requires an explicit region", provider)
	}
	if inferredProvider == ProviderGeneric && !explicitProvider {
		return fmt.Errorf("s3 provider %q endpoint host %q is not recognized", provider, host)
	}
	if cfg.EnableRestore {
		return &ProviderCompatibilityError{
			Provider: provider, Feature: "Glacier restore",
			Reason: "provider profiles do not expose the AWS Glacier restore API",
		}
	}
	if cfg.StorageClass != "" && !strings.EqualFold(cfg.StorageClass, "STANDARD") {
		return &ProviderCompatibilityError{
			Provider: provider, Feature: "storage class " + cfg.StorageClass,
			Reason: "only STANDARD is supported",
		}
	}
	return nil
}

func normalizeBucketLookup(raw, bucket string, provider Provider) (string, error) {
	bucketLookup := strings.ToLower(strings.TrimSpace(raw))
	if bucketLookup == "" || bucketLookup == "auto" {
		if provider == ProviderGeneric {
			bucketLookup = "auto"
		} else {
			bucketLookup = "dns"
		}
	}
	if bucketLookup != "auto" && bucketLookup != "dns" && bucketLookup != "path" {
		return "", fmt.Errorf(`bad bucket-lookup style %q must be "auto", "path" or "dns"`, raw)
	}
	if bucketLookup == "dns" && !dnsCompatibleBucket(bucket) {
		return "", fmt.Errorf("s3 DNS bucket lookup requires a DNS-compatible bucket name, got %q", bucket)
	}
	return bucketLookup, nil
}

func endpointHostname(endpoint string) string {
	host := endpoint
	if parsed, _, err := net.SplitHostPort(endpoint); err == nil {
		host = parsed
	}
	return strings.ToLower(strings.TrimSuffix(host, "."))
}

func inferProvider(host string) (Provider, string) {
	const backblazeSuffix = ".backblazeb2.com"
	const wasabiSuffix = ".wasabisys.com"
	if strings.HasPrefix(host, "s3.") && strings.HasSuffix(host, backblazeSuffix) {
		return ProviderBackblaze, strings.TrimSuffix(strings.TrimPrefix(host, "s3."), backblazeSuffix)
	}
	if strings.HasPrefix(host, "s3.") && strings.HasSuffix(host, wasabiSuffix) {
		region := strings.TrimSuffix(strings.TrimPrefix(host, "s3."), wasabiSuffix)
		if region != "" {
			return ProviderWasabi, region
		}
	}
	if host == "s3.wasabisys.com" {
		return ProviderWasabi, ""
	}
	return ProviderGeneric, ""
}

func providerDomainLookalike(host string) bool {
	return strings.HasSuffix(host, ".backblazeb2.com") || strings.HasSuffix(host, ".wasabisys.com")
}

func dnsCompatibleBucket(bucket string) bool {
	if len(bucket) < 3 || len(bucket) > 63 || strings.Contains(bucket, "..") || net.ParseIP(bucket) != nil {
		return false
	}
	for _, label := range strings.Split(bucket, ".") {
		if label == "" || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, character := range label {
			if (character >= 'a' && character <= 'z') || (character >= '0' && character <= '9') || character == '-' {
				continue
			}
			return false
		}
	}
	return true
}

func providerCapabilities(provider Provider) Capabilities {
	capabilities := Capabilities{
		SignatureV4: true, MultipartUpload: true, RangeReads: true, ListObjectsV2: true,
		ConditionalCreate: "unverified", VersionRetention: "provider-managed", ObjectImmutability: "provider-managed",
		STSRoleAssumption: "probe", GlacierRestore: true,
	}
	switch provider {
	case ProviderGeneric:
	case ProviderBackblaze:
		capabilities.STSRoleAssumption = "unsupported"
		capabilities.GlacierRestore = false
	case ProviderCeph:
		capabilities.STSRoleAssumption = "supported"
		capabilities.GlacierRestore = false
	case ProviderWasabi:
		capabilities.STSRoleAssumption = "supported"
		capabilities.GlacierRestore = false
	}
	return capabilities
}

func roleAssumptionEndpoint(cfg Config, roleARN, configuredEndpoint string) (string, error) {
	if roleARN == "" {
		return "", nil
	}
	provider := Provider(cfg.Provider)
	if provider == ProviderBackblaze {
		return "", &ProviderCompatibilityError{
			Provider: provider, Feature: "AWS STS role assumption",
			Reason: "Backblaze B2 does not expose AWS STS compatibility",
		}
	}
	if provider == ProviderWasabi {
		if configuredEndpoint == "" {
			return "https://sts.wasabisys.com", nil
		}
		parsed, err := url.Parse(configuredEndpoint)
		if err != nil || !strings.EqualFold(parsed.Scheme, "https") ||
			!strings.EqualFold(parsed.Hostname(), "sts.wasabisys.com") || parsed.Port() != "" ||
			parsed.User != nil || (parsed.Path != "" && parsed.Path != "/") || parsed.RawQuery != "" || parsed.Fragment != "" {
			return "", fmt.Errorf("wasabi role assumption requires the HTTPS endpoint https://sts.wasabisys.com")
		}
		return "https://sts.wasabisys.com", nil
	}
	if configuredEndpoint != "" {
		return configuredEndpoint, nil
	}
	if cfg.Region == "" {
		return "https://sts.amazonaws.com", nil
	}
	if strings.HasPrefix(cfg.Region, "cn-") {
		return "https://sts." + cfg.Region + ".amazonaws.com.cn", nil
	}
	return "https://sts." + cfg.Region + ".amazonaws.com", nil
}

type endpointGuardRoundTripper struct {
	base      http.RoundTripper
	authority string
}

func (transport endpointGuardRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := transport.base.RoundTrip(request)
	if err != nil || response.StatusCode < http.StatusMultipleChoices || response.StatusCode >= http.StatusBadRequest {
		return response, err
	}
	location, err := response.Location()
	if err != nil {
		return response, nil
	}
	if !strings.EqualFold(location.Host, transport.authority) {
		_ = response.Body.Close() // Preserve the redirect policy error; response cleanup is best effort.
		return nil, fmt.Errorf("s3 redirect from %q to unvalidated host %q rejected", transport.authority, location.Host)
	}
	return response, nil
}

func guardEndpointRedirects(base http.RoundTripper, endpoint string) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return endpointGuardRoundTripper{base: base, authority: strings.ToLower(strings.TrimSuffix(endpoint, "."))}
}
