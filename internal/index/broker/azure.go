package broker

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const maxProviderResponse = 1024 * 1024

type AzureKeyVaultUnwrapper struct {
	token  string
	client *http.Client
}

func NewAzureKeyVaultUnwrapper(token string, client *http.Client) (*AzureKeyVaultUnwrapper, error) {
	if strings.TrimSpace(token) == "" {
		return nil, errors.New("Azure Key Vault bearer token is required")
	}
	if client == nil {
		client = http.DefaultClient
	}
	providerClient := *client
	providerClient.CheckRedirect = rejectProviderRedirect
	return &AzureKeyVaultUnwrapper{token: strings.TrimSpace(token), client: &providerClient}, nil
}

func rejectProviderRedirect(_ *http.Request, _ []*http.Request) error {
	return errors.New("provider redirects are not allowed")
}

func (unwrapper *AzureKeyVaultUnwrapper) UnwrapMember(ctx context.Context, member ExternalMemberContext, ciphertext []byte) ([]byte, VerifiedPrincipal, error) {
	if member.Provider != "azure-key-vault" || len(ciphertext) == 0 {
		return nil, VerifiedPrincipal{}, errors.New("invalid Azure Key Vault member request")
	}
	keyURL, err := validateAzureKeyURL(member.KeyReference)
	if err != nil {
		return nil, VerifiedPrincipal{}, err
	}
	body, err := json.Marshal(struct {
		Algorithm string `json:"alg"`
		Value     string `json:"value"`
	}{Algorithm: "RSA-OAEP-256", Value: base64.RawURLEncoding.EncodeToString(ciphertext)})
	if err != nil {
		return nil, VerifiedPrincipal{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(keyURL.String(), "/")+"/unwrapkey?api-version=7.4", bytes.NewReader(body))
	if err != nil {
		return nil, VerifiedPrincipal{}, err
	}
	request.Header.Set("Authorization", "Bearer "+unwrapper.token)
	request.Header.Set("Content-Type", "application/json")
	response, err := unwrapper.client.Do(request)
	if err != nil {
		return nil, VerifiedPrincipal{}, fmt.Errorf("Azure Key Vault unwrapKey: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maxProviderResponse))
		return nil, VerifiedPrincipal{}, fmt.Errorf("Azure Key Vault unwrapKey returned HTTP %d", response.StatusCode)
	}
	var result struct {
		Value string `json:"value"`
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, maxProviderResponse))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return nil, VerifiedPrincipal{}, fmt.Errorf("decode Azure Key Vault unwrapKey response: %w", err)
	}
	plaintext, err := base64.RawURLEncoding.DecodeString(result.Value)
	if err != nil || len(plaintext) == 0 {
		return nil, VerifiedPrincipal{}, errors.New("Azure Key Vault returned invalid plaintext")
	}
	principal, err := azureTokenPrincipal(unwrapper.token)
	if err != nil {
		clear(plaintext)
		return nil, VerifiedPrincipal{}, err
	}
	return plaintext, principal, nil
}

func validateAzureKeyURL(reference string) (*url.URL, error) {
	value, err := url.Parse(reference)
	if err != nil {
		return nil, fmt.Errorf("parse Azure key reference: %w", err)
	}
	host := strings.ToLower(value.Hostname())
	segments := strings.Split(strings.Trim(value.EscapedPath(), "/"), "/")
	if value.Scheme != "https" || value.User != nil || value.RawQuery != "" || value.Fragment != "" || (!strings.HasSuffix(host, ".vault.azure.net") && !strings.HasSuffix(host, ".managedhsm.azure.net")) || len(segments) != 3 || segments[0] != "keys" || segments[1] == "" || segments[2] == "" {
		return nil, errors.New("Azure key reference must be a versioned Key Vault or Managed HSM HTTPS URL")
	}
	return value, nil
}

func azureTokenPrincipal(token string) (VerifiedPrincipal, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return VerifiedPrincipal{}, errors.New("Azure bearer token is not a JWT with verifiable provider-bound claims")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return VerifiedPrincipal{}, errors.New("decode Azure bearer token claims")
	}
	var claims struct {
		TenantID string          `json:"tid"`
		ObjectID string          `json:"oid"`
		Audience json.RawMessage `json:"aud"`
		Expires  int64           `json:"exp"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return VerifiedPrincipal{}, errors.New("decode Azure bearer token claims")
	}
	if claims.TenantID == "" || claims.ObjectID == "" || !azureVaultAudience(claims.Audience) || claims.Expires <= time.Now().Unix() {
		return VerifiedPrincipal{}, errors.New("Azure bearer token lacks current Key Vault tenant, object, audience, or expiry claims")
	}
	return VerifiedPrincipal{Authority: "entra", TenantAccountOrProject: claims.TenantID, ImmutablePrincipalID: claims.ObjectID}, nil
}

func azureVaultAudience(raw json.RawMessage) bool {
	var audience string
	if json.Unmarshal(raw, &audience) == nil {
		return audience == "https://vault.azure.net" || audience == "https://vault.azure.net/"
	}
	var audiences []string
	if json.Unmarshal(raw, &audiences) != nil {
		return false
	}
	for _, value := range audiences {
		if value == "https://vault.azure.net" || value == "https://vault.azure.net/" {
			return true
		}
	}
	return false
}
