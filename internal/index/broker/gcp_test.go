package broker

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestGoogleCloudKMSUnwrapperReturnsProviderAcceptedPrincipal(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		switch request.URL.Host {
		case "cloudkms.googleapis.com":
			if request.Method != http.MethodPost || !strings.HasSuffix(request.URL.Path, "/projects/project-a/locations/global/keyRings/ring/cryptoKeys/alice:decrypt") {
				t.Fatalf("unexpected Google KMS request: %s %s", request.Method, request.URL)
			}
			body, err := io.ReadAll(request.Body)
			if err != nil || !strings.Contains(string(body), "additionalAuthenticatedData") {
				t.Fatalf("Google KMS request lacks AAD: %s", body)
			}
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"plaintext":"` + base64.StdEncoding.EncodeToString([]byte("wrapped-share")) + `"}`)), Header: make(http.Header)}, nil
		case "oauth2.googleapis.com":
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"sub":"subject-a","email":"alice@example.com","expires_in":"3600"}`)), Header: make(http.Header)}, nil
		default:
			t.Fatalf("unexpected host %q", request.URL.Host)
			return nil, nil
		}
	})}
	unwrapper, err := NewGoogleCloudKMSUnwrapper([]byte("token-a"), client)
	if err != nil {
		t.Fatal(err)
	}
	plaintext, principal, err := unwrapper.UnwrapMember(context.Background(), ExternalMemberContext{RepositoryID: "repo-a", RootKeyVersion: 1, MemberID: "alice", Provider: "gcp-kms", KeyReference: "projects/project-a/locations/global/keyRings/ring/cryptoKeys/alice", Purpose: "purpose-a"}, []byte("ciphertext"))
	if err != nil {
		t.Fatal(err)
	}
	if string(plaintext) != "wrapped-share" || principal.Authority != "gcp-iam" || principal.TenantAccountOrProject != "project-a" || principal.ImmutablePrincipalID != "subject-a" {
		t.Fatalf("unexpected Google result: %q %+v", plaintext, principal)
	}
}

func TestGoogleCloudKMSUnwrapperFailsClosed(t *testing.T) {
	if _, err := validateGoogleKeyReference("projects/project-a/locations/global/keyRings/ring/cryptoKeys/key/cryptoKeyVersions/1"); err == nil {
		t.Fatal("versioned Google key reference accepted")
	}
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusForbidden, Body: io.NopCloser(strings.NewReader(`{}`)), Header: make(http.Header)}, nil
	})}
	unwrapper, err := NewGoogleCloudKMSUnwrapper([]byte("token-a"), client)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := unwrapper.UnwrapMember(context.Background(), ExternalMemberContext{Provider: "gcp-kms", KeyReference: "projects/project-a/locations/global/keyRings/ring/cryptoKeys/alice"}, []byte("ciphertext")); err == nil {
		t.Fatal("Google provider denial accepted")
	}
}

func TestGoogleCloudHSMRequiresProviderAttestation(t *testing.T) {
	protectionLevel := "HSM"
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Host == "oauth2.googleapis.com" {
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"sub":"subject-a","expires_in":3600}`)), Header: make(http.Header)}, nil
		}
		body := `{"plaintext":"` + base64.StdEncoding.EncodeToString([]byte("wrapped-share")) + `","protectionLevel":"` + protectionLevel + `"}`
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
	})}
	unwrapper, err := NewGoogleCloudKMSUnwrapper([]byte("token-a"), client)
	if err != nil {
		t.Fatal(err)
	}
	member := ExternalMemberContext{RepositoryID: "repo-a", RootKeyVersion: 1, MemberID: "alice", Provider: "gcp-cloud-hsm", KeyReference: "projects/project-a/locations/global/keyRings/ring/cryptoKeys/alice", Purpose: "purpose-a"}
	if _, _, err := unwrapper.UnwrapMember(context.Background(), member, []byte("ciphertext")); err != nil {
		t.Fatal(err)
	}
	protectionLevel = "SOFTWARE"
	if _, _, err := unwrapper.UnwrapMember(context.Background(), member, []byte("ciphertext")); err == nil {
		t.Fatal("software-protected key accepted for Google Cloud HSM member")
	}
}

func TestGoogleTokenIntrospectionRejectsExpiredToken(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Host == "cloudkms.googleapis.com" {
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"plaintext":"` + base64.StdEncoding.EncodeToString([]byte("wrapped-share")) + `"}`)), Header: make(http.Header)}, nil
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"sub":"subject-a","expires_in":"0"}`)), Header: make(http.Header)}, nil
	})}
	unwrapper, err := NewGoogleCloudKMSUnwrapper([]byte("token-a"), client)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = unwrapper.UnwrapMember(context.Background(), ExternalMemberContext{RepositoryID: "repo-a", RootKeyVersion: 1, MemberID: "alice", Provider: "gcp-kms", KeyReference: "projects/project-a/locations/global/keyRings/ring/cryptoKeys/alice", Purpose: "purpose-a"}, []byte("ciphertext"))
	if err == nil || !strings.Contains(err.Error(), "current lifetime") {
		t.Fatalf("expired Google token was accepted: %v", err)
	}
}

func TestGoogleTokenIntrospectionErrorDoesNotLeakToken(t *testing.T) {
	const token = "secret-token-value"
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Host == "cloudkms.googleapis.com" {
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"plaintext":"` + base64.StdEncoding.EncodeToString([]byte("wrapped-share")) + `"}`)), Header: make(http.Header)}, nil
		}
		return nil, errors.New("request failed for " + request.URL.String())
	})}
	unwrapper, err := NewGoogleCloudKMSUnwrapper([]byte(token), client)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = unwrapper.UnwrapMember(context.Background(), ExternalMemberContext{RepositoryID: "repo-a", RootKeyVersion: 1, MemberID: "alice", Provider: "gcp-kms", KeyReference: "projects/project-a/locations/global/keyRings/ring/cryptoKeys/alice", Purpose: "purpose-a"}, []byte("ciphertext"))
	if err == nil || strings.Contains(err.Error(), token) {
		t.Fatalf("token-bearing provider error was not sanitized: %v", err)
	}
}
