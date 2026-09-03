package broker

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

func TestCloudUnwrapperTokensAreClearable(t *testing.T) {
	azure, err := NewAzureKeyVaultUnwrapper([]byte("azure-token"), nil)
	if err != nil {
		t.Fatal(err)
	}
	google, err := NewGoogleCloudKMSUnwrapper([]byte("google-token"), nil)
	if err != nil {
		t.Fatal(err)
	}
	azure.Clear()
	google.Clear()
	for _, token := range [][]byte{azure.token, google.token} {
		for _, value := range token {
			if value != 0 {
				t.Fatal("cloud bearer token was not cleared")
			}
		}
	}
}

func azureTestToken(audience, tenant, object string) string {
	claims := fmt.Sprintf(`{"aud":%q,"tid":%q,"oid":%q,"exp":%d}`, audience, tenant, object, time.Now().Add(time.Hour).Unix())
	return "header." + base64.RawURLEncoding.EncodeToString([]byte(claims)) + ".signature"
}

func TestAzureKeyVaultUnwrapperReturnsProviderAcceptedPrincipal(t *testing.T) {
	token := azureTestToken("https://vault.azure.net", "tenant-a", "object-a")
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodPost || request.URL.Path != "/keys/alice/version/unwrapkey" || request.URL.Query().Get("api-version") != "7.4" {
			t.Fatalf("unexpected Azure request: %s %s", request.Method, request.URL)
		}
		if request.Header.Get("Authorization") != "Bearer "+token {
			t.Fatal("Azure bearer token missing")
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"value":"` + base64.RawURLEncoding.EncodeToString([]byte("wrapped-share")) + `"}`)), Header: make(http.Header)}, nil
	})}
	unwrapper, err := NewAzureKeyVaultUnwrapper([]byte(token), client)
	if err != nil {
		t.Fatal(err)
	}
	plaintext, principal, err := unwrapper.UnwrapMember(context.Background(), ExternalMemberContext{Provider: "azure-key-vault", KeyReference: "https://example.vault.azure.net/keys/alice/version"}, []byte("ciphertext"))
	if err != nil {
		t.Fatal(err)
	}
	if string(plaintext) != "wrapped-share" || principal.Authority != "entra" || principal.TenantAccountOrProject != "tenant-a" || principal.ImmutablePrincipalID != "object-a" {
		t.Fatalf("unexpected Azure result: %q %+v", plaintext, principal)
	}
}

func TestAzureKeyVaultUnwrapperFailsClosed(t *testing.T) {
	if _, err := validateAzureKeyURL("https://example.invalid/keys/alice/version"); err == nil {
		t.Fatal("non-Azure key URL accepted")
	}
	if _, err := azureTokenPrincipal(azureTestToken("other-audience", "tenant-a", "object-a")); err == nil {
		t.Fatal("wrong Azure token audience accepted")
	}
	expiredClaims := fmt.Sprintf(`{"aud":"https://vault.azure.net","tid":"tenant-a","oid":"object-a","exp":%d}`, time.Now().Add(-time.Minute).Unix())
	expiredToken := "header." + base64.RawURLEncoding.EncodeToString([]byte(expiredClaims)) + ".signature"
	if _, err := azureTokenPrincipal(expiredToken); err == nil {
		t.Fatal("expired Azure bearer token accepted")
	}
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusForbidden, Body: io.NopCloser(strings.NewReader(`{"error":"denied"}`)), Header: make(http.Header)}, nil
	})}
	unwrapper, err := NewAzureKeyVaultUnwrapper([]byte(azureTestToken("https://vault.azure.net", "tenant-a", "object-a")), client)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := unwrapper.UnwrapMember(context.Background(), ExternalMemberContext{Provider: "azure-key-vault", KeyReference: "https://example.vault.azure.net/keys/alice/version"}, []byte("ciphertext")); err == nil {
		t.Fatal("Azure provider denial accepted")
	}
}

func TestProviderRedirectsAreRejected(t *testing.T) {
	unwrapper, err := NewAzureKeyVaultUnwrapper([]byte("token"), &http.Client{})
	if err != nil {
		t.Fatal(err)
	}
	if err := unwrapper.client.CheckRedirect(&http.Request{}, nil); err == nil {
		t.Fatal("provider redirect accepted")
	}
}
