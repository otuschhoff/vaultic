package keyvault

import (
	"context"
	"fmt"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azsecrets"
)

type secretClient interface {
	GetSecret(context.Context, string, string, *azsecrets.GetSecretOptions) (azsecrets.GetSecretResponse, error)
}

var newSecretClient = func(vaultURL string) (secretClient, error) {
	credential, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("create DefaultAzureCredential: %w", err)
	}
	client, err := azsecrets.NewClient(vaultURL, credential, nil)
	if err != nil {
		return nil, fmt.Errorf("create Azure Key Vault client: %w", err)
	}
	return client, nil
}

func FetchSecret(ctx context.Context, vaultURL, name, version string) (string, error) {
	if strings.TrimSpace(vaultURL) == "" || strings.TrimSpace(name) == "" {
		return "", fmt.Errorf("Azure Key Vault URL and secret name are required")
	}
	client, err := newSecretClient(vaultURL)
	if err != nil {
		return "", err
	}
	response, err := client.GetSecret(ctx, name, version, nil)
	if err != nil {
		return "", fmt.Errorf("SecretGet %q: %w", name, err)
	}
	if response.Value == nil || *response.Value == "" {
		return "", fmt.Errorf("SecretGet %q returned an empty value", name)
	}
	return *response.Value, nil
}
