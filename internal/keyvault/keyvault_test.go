package keyvault

import (
	"context"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azsecrets"
)

type fakeSecretClient struct {
	calls   int
	name    string
	version string
	value   *string
	err     error
}

func (client *fakeSecretClient) GetSecret(_ context.Context, name, version string, _ *azsecrets.GetSecretOptions) (azsecrets.GetSecretResponse, error) {
	client.calls++
	client.name, client.version = name, version
	return azsecrets.GetSecretResponse{Secret: azsecrets.Secret{Value: client.value}}, client.err
}

func TestFetchSecretOnce(t *testing.T) {
	value := "repository-passphrase"
	fake := &fakeSecretClient{value: &value}
	previous := newSecretClient
	newSecretClient = func(string) (secretClient, error) { return fake, nil }
	t.Cleanup(func() { newSecretClient = previous })

	got, err := FetchSecret(context.Background(), "https://example.vault.azure.net", "vaultic", "v1")
	if err != nil {
		t.Fatal(err)
	}
	if got != value || fake.calls != 1 || fake.name != "vaultic" || fake.version != "v1" {
		t.Fatalf("SecretGet = %q, calls=%d, name=%q, version=%q", got, fake.calls, fake.name, fake.version)
	}
}

func TestFetchSecretRejectsEmptyValue(t *testing.T) {
	fake := &fakeSecretClient{}
	previous := newSecretClient
	newSecretClient = func(string) (secretClient, error) { return fake, nil }
	t.Cleanup(func() { newSecretClient = previous })

	if _, err := FetchSecret(context.Background(), "https://example.vault.azure.net", "vaultic", ""); err == nil {
		t.Fatal("expected an empty secret error")
	}
}
