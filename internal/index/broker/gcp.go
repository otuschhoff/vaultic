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
	"strconv"
	"strings"
)

type GoogleCloudKMSUnwrapper struct {
	token  string
	client *http.Client
}

func NewGoogleCloudKMSUnwrapper(token string, client *http.Client) (*GoogleCloudKMSUnwrapper, error) {
	if strings.TrimSpace(token) == "" {
		return nil, errors.New("Google Cloud bearer token is required")
	}
	if client == nil {
		client = http.DefaultClient
	}
	providerClient := *client
	providerClient.CheckRedirect = rejectProviderRedirect
	return &GoogleCloudKMSUnwrapper{token: strings.TrimSpace(token), client: &providerClient}, nil
}

func (unwrapper *GoogleCloudKMSUnwrapper) UnwrapMember(ctx context.Context, member ExternalMemberContext, ciphertext []byte) ([]byte, VerifiedPrincipal, error) {
	project, err := validateGoogleKeyReference(member.KeyReference)
	if err != nil {
		return nil, VerifiedPrincipal{}, err
	}
	if member.Provider != "gcp-kms" && member.Provider != "gcp-cloud-hsm" {
		return nil, VerifiedPrincipal{}, errors.New("invalid Google Cloud KMS member request")
	}
	binding := fmt.Sprintf("vaulticdb\x00%s\x00%s\x00%d\x00%s", member.RepositoryID, member.MemberID, member.RootKeyVersion, member.Purpose)
	body, err := json.Marshal(struct {
		Ciphertext                  string `json:"ciphertext"`
		AdditionalAuthenticatedData string `json:"additionalAuthenticatedData"`
	}{Ciphertext: base64.StdEncoding.EncodeToString(ciphertext), AdditionalAuthenticatedData: base64.StdEncoding.EncodeToString([]byte(binding))})
	if err != nil {
		return nil, VerifiedPrincipal{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://cloudkms.googleapis.com/v1/"+member.KeyReference+":decrypt", bytes.NewReader(body))
	if err != nil {
		return nil, VerifiedPrincipal{}, err
	}
	request.Header.Set("Authorization", "Bearer "+unwrapper.token)
	request.Header.Set("Content-Type", "application/json")
	response, err := unwrapper.client.Do(request)
	if err != nil {
		return nil, VerifiedPrincipal{}, fmt.Errorf("Google Cloud KMS decrypt: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maxProviderResponse))
		return nil, VerifiedPrincipal{}, fmt.Errorf("Google Cloud KMS decrypt returned HTTP %d", response.StatusCode)
	}
	var result struct {
		Plaintext       string `json:"plaintext"`
		ProtectionLevel string `json:"protectionLevel"`
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, maxProviderResponse))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return nil, VerifiedPrincipal{}, fmt.Errorf("decode Google Cloud KMS response: %w", err)
	}
	if member.Provider == "gcp-cloud-hsm" && result.ProtectionLevel != "HSM" {
		return nil, VerifiedPrincipal{}, errors.New("Google Cloud HSM member key is not HSM protected")
	}
	plaintext, err := base64.StdEncoding.DecodeString(result.Plaintext)
	if err != nil || len(plaintext) == 0 {
		return nil, VerifiedPrincipal{}, errors.New("Google Cloud KMS returned invalid plaintext")
	}
	principal, err := unwrapper.lookupPrincipal(ctx, project)
	if err != nil {
		clear(plaintext)
		return nil, VerifiedPrincipal{}, err
	}
	return plaintext, principal, nil
}

func validateGoogleKeyReference(reference string) (string, error) {
	segments := strings.Split(reference, "/")
	if len(segments) != 8 || segments[0] != "projects" || segments[1] == "" || segments[2] != "locations" || segments[3] == "" || segments[4] != "keyRings" || segments[5] == "" || segments[6] != "cryptoKeys" || segments[7] == "" {
		return "", errors.New("Google Cloud KMS key reference must name a CryptoKey")
	}
	return segments[1], nil
}

func (unwrapper *GoogleCloudKMSUnwrapper) lookupPrincipal(ctx context.Context, project string) (VerifiedPrincipal, error) {
	endpoint := "https://oauth2.googleapis.com/tokeninfo?access_token=" + url.QueryEscape(unwrapper.token)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return VerifiedPrincipal{}, err
	}
	response, err := unwrapper.client.Do(request)
	if err != nil {
		return VerifiedPrincipal{}, errors.New("Google token introspection request failed")
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return VerifiedPrincipal{}, fmt.Errorf("Google token introspection returned HTTP %d", response.StatusCode)
	}
	var claims struct {
		Subject   string          `json:"sub"`
		Email     string          `json:"email"`
		ExpiresIn json.RawMessage `json:"expires_in"`
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, maxProviderResponse))
	if err := decoder.Decode(&claims); err != nil {
		return VerifiedPrincipal{}, fmt.Errorf("decode Google token identity: %w", err)
	}
	identity := claims.Subject
	if identity == "" {
		identity = claims.Email
	}
	if identity == "" || !positiveGoogleTokenLifetime(claims.ExpiresIn) {
		return VerifiedPrincipal{}, errors.New("Google token identity has no immutable subject or current lifetime")
	}
	return VerifiedPrincipal{Authority: "gcp-iam", TenantAccountOrProject: project, ImmutablePrincipalID: identity}, nil
}

func positiveGoogleTokenLifetime(raw json.RawMessage) bool {
	var seconds int64
	if json.Unmarshal(raw, &seconds) == nil {
		return seconds > 0
	}
	var text string
	if json.Unmarshal(raw, &text) != nil {
		return false
	}
	seconds, err := strconv.ParseInt(text, 10, 64)
	return err == nil && seconds > 0
}
