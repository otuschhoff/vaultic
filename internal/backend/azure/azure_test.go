package azure_test

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/vaultic/vaultic/internal/backend"
	"github.com/vaultic/vaultic/internal/backend/azure"
	"github.com/vaultic/vaultic/internal/backend/test"
	"github.com/vaultic/vaultic/internal/options"
	rtest "github.com/vaultic/vaultic/internal/test"
	"github.com/vaultic/vaultic/internal/vaultic"
)

func newAzureTestSuite() *test.Suite[azure.Config] {
	return &test.Suite[azure.Config]{
		// do not use excessive data
		MinimalData: true,

		// NewConfig returns a config for a new temporary backend that will be used in tests.
		NewConfig: func() (*azure.Config, error) {
			cfg, err := azure.ParseConfig(os.Getenv("VAULTIC_TEST_AZURE_REPOSITORY"))
			if err != nil {
				return nil, err
			}

			cfg.ApplyEnvironment("VAULTIC_TEST_")
			cfg.Prefix = fmt.Sprintf("test-%d", time.Now().UnixNano())
			return cfg, nil
		},

		Factory: azure.NewFactory(),
	}
}

func TestBackendAzure(t *testing.T) {
	defer func() {
		if t.Skipped() {
			rtest.SkipDisallowed(t, "vaultic/backend/azure.TestBackendAzure")
		}
	}()

	vars := []string{
		"VAULTIC_TEST_AZURE_ACCOUNT_NAME",
		"VAULTIC_TEST_AZURE_ACCOUNT_KEY",
		"VAULTIC_TEST_AZURE_REPOSITORY",
	}

	for _, v := range vars {
		if os.Getenv(v) == "" {
			t.Skipf("environment variable %v not set", v)
			return
		}
	}

	t.Logf("run tests")
	newAzureTestSuite().RunTests(t)
}

func BenchmarkBackendAzure(t *testing.B) {
	vars := []string{
		"VAULTIC_TEST_AZURE_ACCOUNT_NAME",
		"VAULTIC_TEST_AZURE_ACCOUNT_KEY",
		"VAULTIC_TEST_AZURE_REPOSITORY",
	}

	for _, v := range vars {
		if os.Getenv(v) == "" {
			t.Skipf("environment variable %v not set", v)
			return
		}
	}

	t.Logf("run tests")
	newAzureTestSuite().RunBenchmarks(t)
}

// TestBackendAzureAccountToken tests that a Storage Account SAS/SAT token can authorize.
// This test ensures that vaultic can use a token that was generated using the storage
// account keys can be used to authorize the azure connection.
// Requires the VAULTIC_TEST_AZURE_ACCOUNT_NAME, VAULTIC_TEST_AZURE_REPOSITORY, and the
// VAULTIC_TEST_AZURE_ACCOUNT_SAS environment variables to be set, otherwise this test
// will be skipped.
func TestBackendAzureAccountToken(t *testing.T) {
	vars := []string{
		"VAULTIC_TEST_AZURE_ACCOUNT_NAME",
		"VAULTIC_TEST_AZURE_REPOSITORY",
		"VAULTIC_TEST_AZURE_ACCOUNT_SAS",
	}

	for _, v := range vars {
		if os.Getenv(v) == "" {
			t.Skipf("set %v to test SAS/SAT Token Authentication", v)
			return
		}
	}

	ctx := t.Context()

	cfg, err := azure.ParseConfig(os.Getenv("VAULTIC_TEST_AZURE_REPOSITORY"))
	if err != nil {
		t.Fatal(err)
	}

	cfg.AccountName = os.Getenv("VAULTIC_TEST_AZURE_ACCOUNT_NAME")
	cfg.AccountSAS = options.NewSecretString(os.Getenv("VAULTIC_TEST_AZURE_ACCOUNT_SAS"))

	tr, err := backend.Transport(backend.TransportOptions{})
	if err != nil {
		t.Fatal(err)
	}

	_, err = azure.Create(ctx, *cfg, tr, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
}

// TestBackendAzureContainerToken tests that a container SAS/SAT token can authorize.
// This test ensures that vaultic can use a token that was generated using a user
// delegation key against the container we are storing data in can be used to
// authorize the azure connection.
// Requires the VAULTIC_TEST_AZURE_ACCOUNT_NAME, VAULTIC_TEST_AZURE_REPOSITORY, and the
// VAULTIC_TEST_AZURE_CONTAINER_SAS environment variables to be set, otherwise this test
// will be skipped.
func TestBackendAzureContainerToken(t *testing.T) {
	vars := []string{
		"VAULTIC_TEST_AZURE_ACCOUNT_NAME",
		"VAULTIC_TEST_AZURE_REPOSITORY",
		"VAULTIC_TEST_AZURE_CONTAINER_SAS",
	}

	for _, v := range vars {
		if os.Getenv(v) == "" {
			t.Skipf("set %v to test SAS/SAT Token Authentication", v)
			return
		}
	}

	ctx := t.Context()

	cfg, err := azure.ParseConfig(os.Getenv("VAULTIC_TEST_AZURE_REPOSITORY"))
	if err != nil {
		t.Fatal(err)
	}

	cfg.AccountName = os.Getenv("VAULTIC_TEST_AZURE_ACCOUNT_NAME")
	cfg.AccountSAS = options.NewSecretString(os.Getenv("VAULTIC_TEST_AZURE_CONTAINER_SAS"))

	tr, err := backend.Transport(backend.TransportOptions{})
	if err != nil {
		t.Fatal(err)
	}

	_, err = azure.Create(ctx, *cfg, tr, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
}

func TestUploadLargeFile(t *testing.T) {
	if os.Getenv("VAULTIC_AZURE_TEST_LARGE_UPLOAD") == "" {
		t.Skip("set VAULTIC_AZURE_TEST_LARGE_UPLOAD=1 to test large uploads")
		return
	}

	ctx := t.Context()

	if os.Getenv("VAULTIC_TEST_AZURE_REPOSITORY") == "" {
		t.Skipf("environment variables not available")
		return
	}

	cfg, err := azure.ParseConfig(os.Getenv("VAULTIC_TEST_AZURE_REPOSITORY"))
	if err != nil {
		t.Fatal(err)
	}

	cfg.AccountName = os.Getenv("VAULTIC_TEST_AZURE_ACCOUNT_NAME")
	cfg.AccountKey = options.NewSecretString(os.Getenv("VAULTIC_TEST_AZURE_ACCOUNT_KEY"))
	cfg.Prefix = fmt.Sprintf("test-upload-large-%d", time.Now().UnixNano())

	tr, err := backend.Transport(backend.TransportOptions{})
	if err != nil {
		t.Fatal(err)
	}

	be, err := azure.Create(ctx, *cfg, tr, t.Logf)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		err := be.Delete(ctx)
		if err != nil {
			t.Fatal(err)
		}
	}()

	data := rtest.Random(23, 300*1024*1024)
	id := vaultic.Hash(data)
	h := backend.Handle{Name: id.String(), Type: backend.PackFile}

	t.Logf("hash of %d bytes: %v", len(data), id)

	err = be.Save(ctx, h, backend.NewByteReader(data, be.Hasher()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := be.Remove(ctx, h)
		if err != nil {
			t.Fatal(err)
		}
	}()

	var tests = []struct {
		offset, length int
	}{
		{0, len(data)},
		{23, 1024},
		{23 + 100*1024, 500},
		{888 + 200*1024, 89999},
		{888 + 100*1024*1024, 120 * 1024 * 1024},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			want := data[test.offset : test.offset+test.length]

			buf := make([]byte, test.length)
			err = be.Load(ctx, h, test.length, int64(test.offset), func(rd io.Reader) error {
				_, err = io.ReadFull(rd, buf)
				return err
			})
			if err != nil {
				t.Fatal(err)
			}

			if !bytes.Equal(buf, want) {
				t.Fatalf("wrong bytes returned")
			}
		})
	}
}
