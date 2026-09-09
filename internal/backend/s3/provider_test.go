package s3

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7"
	minioCredentials "github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/otuschhoff/vaultic/internal/options"
)

func TestNormalizeConfigProviderProfiles(t *testing.T) {
	tests := []struct {
		name     string
		cfg      Config
		provider Provider
		region   string
		lookup   string
		wantErr  string
	}{
		{
			name: "generic unchanged", cfg: Config{
				Endpoint: "storage.example", Bucket: "bucket", Region: "region", BucketLookup: "auto",
			}, provider: ProviderGeneric, region: "region", lookup: "auto",
		},
		{
			name: "explicit generic suppresses inference", cfg: Config{
				Provider: "generic", Endpoint: "s3.us-west-004.backblazeb2.com", Bucket: "bucket", Region: "custom",
			}, provider: ProviderGeneric, region: "custom", lookup: "auto",
		},
		{
			name: "infer backblaze", cfg: Config{
				Endpoint: "s3.us-west-004.backblazeb2.com", Bucket: "bucket",
			}, provider: ProviderBackblaze, region: "us-west-004", lookup: "dns",
		},
		{
			name: "infer wasabi", cfg: Config{
				Endpoint: "s3.eu-central-2.wasabisys.com", Bucket: "bucket",
			}, provider: ProviderWasabi, region: "eu-central-2", lookup: "dns",
		},
		{
			name: "explicit custom", cfg: Config{
				Provider: "wasabi", Endpoint: "objects.example", Bucket: "bucket", Region: "eu-central-2",
			}, provider: ProviderWasabi, region: "eu-central-2", lookup: "dns",
		},
		{
			name: "explicit ceph", cfg: Config{
				Provider: "ceph", Endpoint: "rgw.example", Bucket: "bucket", Region: "us-east-1",
			}, provider: ProviderCeph, region: "us-east-1", lookup: "dns",
		},
		{
			name: "explicit lookup wins", cfg: Config{
				Provider: "backblaze", Endpoint: "s3.us-west-004.backblazeb2.com", Bucket: "bucket", BucketLookup: "path",
			}, provider: ProviderBackblaze, region: "us-west-004", lookup: "path",
		},
		{
			name: "list V1 preserved", cfg: Config{
				Provider: "wasabi", Endpoint: "s3.eu-central-2.wasabisys.com", Bucket: "bucket", ListObjectsV1: true,
			}, provider: ProviderWasabi, region: "eu-central-2", lookup: "dns",
		},
		{name: "unknown provider", cfg: Config{Provider: "other", Endpoint: "storage.example", Bucket: "bucket"}, wantErr: "bad S3 provider"},
		{name: "provider mismatch", cfg: Config{Provider: "wasabi", Endpoint: "s3.us-west-004.backblazeb2.com", Bucket: "bucket"}, wantErr: "does not match"},
		{
			name: "provider region typo rejected", cfg: Config{
				Provider: "backblaze", Endpoint: "s3.typo.backblazeb2.com", Bucket: "bucket", Region: "us-west-004",
			}, wantErr: "does not match configured region",
		},
		{
			name: "provider lookalike rejected", cfg: Config{
				Provider: "backblaze", Endpoint: "api.us-west-004.backblazeb2.com", Bucket: "bucket", Region: "us-west-004",
			}, wantErr: "not a valid provider endpoint",
		},
		{
			name: "region mismatch", cfg: Config{
				Provider: "backblaze", Endpoint: "s3.us-west-004.backblazeb2.com", Bucket: "bucket", Region: "wrong",
			}, wantErr: "does not match configured region",
		},
		{
			name: "custom region required", cfg: Config{
				Provider: "wasabi", Endpoint: "objects.example", Bucket: "bucket",
			}, wantErr: "requires an explicit region",
		},
		{name: "production HTTP rejected", cfg: Config{Endpoint: "s3.eu-central-2.wasabisys.com", Bucket: "bucket", UseHTTP: true}, wantErr: "requires HTTPS"},
		{
			name: "invalid DNS bucket", cfg: Config{
				Provider: "wasabi", Endpoint: "s3.eu-central-2.wasabisys.com", Bucket: "Bad_Bucket",
			}, wantErr: "DNS-compatible",
		},
		{name: "IPv4 DNS bucket", cfg: Config{Provider: "wasabi", Endpoint: "s3.eu-central-2.wasabisys.com", Bucket: "192.168.1.1"}, wantErr: "DNS-compatible"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg, profile, err := NormalizeConfig(test.cfg)
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("NormalizeConfig() error = %v, want containing %q", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if profile.Provider != test.provider || cfg.Region != test.region || cfg.BucketLookup != test.lookup {
				t.Fatalf("NormalizeConfig() = %#v, %#v", cfg, profile)
			}
			if test.cfg.ListObjectsV1 && !cfg.ListObjectsV1 {
				t.Fatal("NormalizeConfig() cleared explicit ListObjectsV1")
			}
		})
	}
}

func TestEndpointGuardRejectsCrossHostRedirect(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		t.Fatal("redirect target received authorization")
	}))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Redirect(writer, request, target.URL, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	endpoint, err := url.Parse(redirect.URL)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, redirect.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "must-not-leak")
	response, err := guardEndpointRedirects(http.DefaultTransport, endpoint.Host).RoundTrip(request)
	if response != nil {
		_ = response.Body.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "unvalidated host") {
		t.Fatalf("RoundTrip() error = %v", err)
	}
}

func TestProviderErrorClassification(t *testing.T) {
	for _, test := range []struct {
		code      string
		kind      ErrorKind
		retryable bool
	}{
		{code: "AccessDenied", kind: ErrorAuthentication},
		{code: "SlowDown", kind: ErrorThrottling, retryable: true},
		{code: "QuotaExceeded", kind: ErrorQuota},
		{code: "AccessDeniedByObjectLock", kind: ErrorRetention},
		{code: "NotImplemented", kind: ErrorUnsupportedAPI},
	} {
		err := classifyProviderError("wasabi", minio.ErrorResponse{Code: test.code})
		var providerError *ProviderError
		if !errors.As(err, &providerError) || providerError.Kind != test.kind || isRetryableProviderCode(test.code) != test.retryable {
			t.Fatalf("classifyProviderError(%q) = %#v", test.code, err)
		}
	}
}

func TestStorageProfileAndErrorsDoNotExposeCredentialsOrPrefix(t *testing.T) {
	config := Config{
		Provider: "wasabi", Endpoint: "s3.eu-central-2.wasabisys.com", Region: "eu-central-2",
		Bucket: "bucket", Prefix: "private/customer/repository", BucketLookup: "dns",
		KeyID: "ACCESS-KEY-MUST-NOT-LEAK", Secret: options.NewSecretString("SECRET-MUST-NOT-LEAK"),
	}
	backend := &s3{cfg: config, conditionalCreate: "unverified"}
	encoded, err := json.Marshal(backend.Properties().StorageProfile)
	if err != nil {
		t.Fatal(err)
	}
	providerError := classifyProviderError(config.Provider, minio.ErrorResponse{
		Code: "InvalidAccessKeyId", Message: config.KeyID + config.Secret.Unwrap(),
	})
	output := string(encoded) + providerError.Error()
	for _, secret := range []string{config.KeyID, config.Secret.Unwrap(), config.Prefix} {
		if strings.Contains(output, secret) {
			t.Fatalf("provider status or error leaked %q: %s", secret, output)
		}
	}
}

func TestBackblazeRejectsRoleAssumptionBeforeCredentials(t *testing.T) {
	t.Setenv("VAULTIC_AWS_ASSUME_ROLE_ARN", "arn:aws:iam::123456789012:role/must-not-be-used")
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	_, err := open(Config{
		Provider: "backblaze", Endpoint: "objects.example", Region: "us-west-004", Bucket: "bucket",
	}, http.DefaultTransport)
	var compatibilityError *ProviderCompatibilityError
	if !errors.As(err, &compatibilityError) || compatibilityError.Feature != "AWS STS role assumption" {
		t.Fatalf("open() error = %v, want provider compatibility error", err)
	}
}

func TestWasabiRoleAssumptionEndpoint(t *testing.T) {
	cfg := Config{Provider: "wasabi", Region: "eu-central-2"}
	const roleARN = "arn:aws:iam::123456789012:role/vaultic"
	endpoint, err := roleAssumptionEndpoint(cfg, roleARN, "")
	if err != nil {
		t.Fatal(err)
	}
	if endpoint != "https://sts.wasabisys.com" {
		t.Fatalf("Wasabi STS endpoint = %q", endpoint)
	}
	for _, invalid := range []string{
		"http://sts.wasabisys.com", "https://sts.wasabisys.com.evil.example",
		"https://user@sts.wasabisys.com", "https://sts.wasabisys.com/path",
	} {
		if _, err := roleAssumptionEndpoint(cfg, roleARN, invalid); err == nil {
			t.Fatalf("unsafe Wasabi STS endpoint %q was accepted", invalid)
		}
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (fn roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

func TestWasabiAssumeRoleUsesNativeSTS(t *testing.T) {
	t.Setenv("VAULTIC_AWS_ASSUME_ROLE_SESSION_NAME", "")
	t.Setenv("VAULTIC_AWS_ASSUME_ROLE_EXTERNAL_ID", "")
	t.Setenv("VAULTIC_AWS_ASSUME_ROLE_POLICY", "")
	const roleARN = "arn:aws:iam::123456789012:role/vaultic"
	cfg := Config{
		Provider: "wasabi", Region: "eu-central-2", KeyID: "base-access-key",
		Secret: options.NewSecretString("base-secret-key"),
	}
	credentials, err := getCredentials(cfg, http.DefaultTransport, roleARN, "https://sts.wasabisys.com")
	if err != nil {
		t.Fatal(err)
	}
	transport := roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.String() != "https://sts.wasabisys.com/" {
			t.Fatalf("STS request URL = %q", request.URL)
		}
		if err := request.ParseForm(); err != nil {
			t.Fatal(err)
		}
		if request.Form.Get("Action") != "AssumeRole" || request.Form.Get("RoleArn") != roleARN ||
			request.Form.Get("RoleSessionName") != "vaultic" {
			t.Fatalf("STS request form = %#v", request.Form)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body: io.NopCloser(strings.NewReader(`
<AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <AssumeRoleResult><Credentials>
    <AccessKeyId>temporary-access</AccessKeyId>
    <SecretAccessKey>temporary-secret</SecretAccessKey>
    <SessionToken>temporary-token</SessionToken>
    <Expiration>2099-01-01T00:00:00Z</Expiration>
  </Credentials></AssumeRoleResult>
</AssumeRoleResponse>`)),
			Request: request,
		}, nil
	})
	value, err := credentials.GetWithContext(&minioCredentials.CredContext{
		Client: &http.Client{Transport: transport},
	})
	if err != nil {
		t.Fatal(err)
	}
	if value.AccessKeyID != "temporary-access" || value.SessionToken != "temporary-token" {
		t.Fatalf("temporary credentials = %#v", value)
	}
	if providerCapabilities(ProviderWasabi).STSRoleAssumption != "supported" {
		t.Fatal("Wasabi profile does not advertise native STS role assumption")
	}
}

func TestBrokerSessionCredentialIncludesToken(t *testing.T) {
	configured := Config{
		KeyID: "temporary-access", Secret: options.NewSecretString("temporary-secret"),
		SessionToken: "temporary-token", BrokerCredential: true,
	}
	credentials, err := getCredentials(configured, http.DefaultTransport, "", "")
	if err != nil {
		t.Fatal(err)
	}
	value, err := credentials.GetWithContext(&minioCredentials.CredContext{Client: http.DefaultClient})
	if err != nil {
		t.Fatal(err)
	}
	if value.AccessKeyID != "temporary-access" || value.SecretAccessKey != "temporary-secret" ||
		value.SessionToken != "temporary-token" {
		t.Fatalf("broker session credentials = %#v", value)
	}
}

func TestNormalizeConfigRejectsUnsupportedProviderFeatures(t *testing.T) {
	for _, cfg := range []Config{
		{Provider: "backblaze", Endpoint: "s3.us-west-004.backblazeb2.com", Bucket: "bucket", EnableRestore: true},
		{Provider: "wasabi", Endpoint: "s3.eu-central-2.wasabisys.com", Bucket: "bucket", StorageClass: "GLACIER"},
	} {
		_, _, err := NormalizeConfig(cfg)
		var compatibilityError *ProviderCompatibilityError
		if !errors.As(err, &compatibilityError) {
			t.Fatalf("NormalizeConfig() error = %T %v, want ProviderCompatibilityError", err, err)
		}
	}
}
