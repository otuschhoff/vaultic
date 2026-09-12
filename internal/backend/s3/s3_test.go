package s3_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/backend/location"
	"github.com/otuschhoff/vaultic/internal/backend/s3"
	"github.com/otuschhoff/vaultic/internal/backend/test"
	"github.com/otuschhoff/vaultic/internal/env"
	"github.com/otuschhoff/vaultic/internal/options"
	rtest "github.com/otuschhoff/vaultic/internal/test"
)

func mkdir(t testing.TB, dir string) {
	err := os.MkdirAll(dir, 0700)
	if err != nil {
		t.Fatal(err)
	}
}

func runMinio(ctx context.Context, t testing.TB, dir, key, secret string) (string, func()) {
	mkdir(t, filepath.Join(dir, "config"))
	mkdir(t, filepath.Join(dir, "root"))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}

	cmd := exec.CommandContext(ctx, "minio",
		"server", "--address", address,
		"--console-address", "127.0.0.1:0",
		"--config-dir", filepath.Join(dir, "config"),
		filepath.Join(dir, "root"))
	cmd.Env = append(os.Environ(),
		"MINIO_ROOT_USER="+key,
		"MINIO_ROOT_PASSWORD="+secret,
	)
	cmd.Stderr = os.Stderr

	err = cmd.Start()
	if err != nil {
		t.Fatal(err)
	}

	// wait until the TCP port is reachable
	var success bool
	for range 100 {
		time.Sleep(200 * time.Millisecond)

		c, err := net.Dial("tcp", address)
		if err == nil {
			success = true
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
			break
		}
	}

	if !success {
		t.Fatal("unable to connect to minio server")
		return "", nil
	}

	return address, func() {
		err = cmd.Process.Kill()
		if err != nil {
			t.Fatal(err)
		}

		// ignore errors, we've killed the process
		_ = cmd.Wait()
	}
}

func newRandomCredentials(t testing.TB) (key, secret string) {
	buf := make([]byte, 10)
	_, err := io.ReadFull(rand.Reader, buf)
	if err != nil {
		t.Fatal(err)
	}
	key = hex.EncodeToString(buf)

	_, err = io.ReadFull(rand.Reader, buf)
	if err != nil {
		t.Fatal(err)
	}
	secret = hex.EncodeToString(buf)

	return key, secret
}

func newMinioTestSuite(t testing.TB, provider string) (*test.Suite[s3.Config], func()) {
	ctx, cancel := context.WithCancel(context.Background())

	tempdir := rtest.TempDir(t)
	key, secret := newRandomCredentials(t)
	endpoint, cleanup := runMinio(ctx, t, tempdir, key, secret)

	return &test.Suite[s3.Config]{
		// NewConfig returns a config for a new temporary backend that will be used in tests.
		NewConfig: func() (*s3.Config, error) {
			cfg := s3.NewConfig()
			cfg.Endpoint = endpoint
			cfg.Bucket = "vaultictestbucket"
			cfg.Prefix = fmt.Sprintf("test-%d", time.Now().UnixNano())
			cfg.UseHTTP = true
			cfg.Provider = provider
			if provider != "" {
				cfg.Region = "test-region"
				cfg.BucketLookup = "path"
			}
			cfg.KeyID = key
			cfg.Secret = options.NewSecretString(secret)
			return &cfg, nil
		},

		Factory: location.NewHTTPBackendFactory(
			"s3",
			s3.ParseConfig,
			location.NoPassword,
			func(ctx context.Context, cfg s3.Config, rt http.RoundTripper, errorLog func(string, ...any)) (be backend.Backend, err error) {
				for i := range 50 {
					be, err = s3.Create(ctx, cfg, rt, errorLog)
					if err != nil {
						t.Logf("s3 open: try %d: error %v", i, err)
						time.Sleep(500 * time.Millisecond)
						continue
					}
					break
				}
				return be, err
			},
			s3.Open,
		),
	}, func() {
		defer cancel()
		defer cleanup()
	}
}

func TestBackendMinio(t *testing.T) {
	defer func() {
		if t.Skipped() {
			rtest.SkipDisallowed(t, "vaultic/backend/s3.TestBackendMinio")
		}
	}()

	// try to find a minio binary
	_, err := exec.LookPath("minio")
	if err != nil {
		t.Skip(err)
		return
	}

	suite, cleanup := newMinioTestSuite(t, "")
	defer cleanup()

	suite.RunTests(t)
}

func TestBackendProviderProfilesMinio(t *testing.T) {
	if _, err := exec.LookPath("minio"); err != nil {
		t.Skip(err)
	}
	for _, provider := range []string{"backblaze", "wasabi"} {
		t.Run(provider, func(t *testing.T) {
			suite, cleanup := newMinioTestSuite(t, provider)
			defer cleanup()
			probeSuiteCapabilities(t, suite, true)
			suite.RunTests(t)
		})
	}
}

func probeSuiteCapabilities(t *testing.T, suite *test.Suite[s3.Config], requireStrict bool) {
	config, err := suite.NewConfig()
	if err != nil {
		t.Fatal(err)
	}
	transport, err := backend.Transport(backend.TransportOptions{})
	if err != nil {
		t.Fatal(err)
	}
	store, err := s3.Create(t.Context(), *config, transport, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	prober, ok := store.(backend.StorageCapabilityProber)
	if !ok {
		t.Fatal("S3 backend does not expose storage capability probing")
	}
	profile, err := prober.ProbeStorageCapabilities(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if requireStrict && profile.ConditionalCreate != "strict" {
		t.Fatalf("conditional create = %q, want strict", profile.ConditionalCreate)
	}
	if profile.ConditionalCreate == "unverified" {
		t.Fatal("conditional create remained unverified after probe")
	}
}

func BenchmarkBackendMinio(t *testing.B) {
	// try to find a minio binary
	_, err := exec.LookPath("minio")
	if err != nil {
		t.Skip(err)
		return
	}

	suite, cleanup := newMinioTestSuite(t, "")
	defer cleanup()

	suite.RunBenchmarks(t)
}

func newS3TestSuite() *test.Suite[s3.Config] {
	return &test.Suite[s3.Config]{
		// do not use excessive data
		MinimalData: true,

		// NewConfig returns a config for a new temporary backend that will be used in tests.
		NewConfig: func() (*s3.Config, error) {
			cfg, err := s3.ParseConfig(env.Get("TEST_S3_REPOSITORY"))
			if err != nil {
				return nil, err
			}

			cfg.KeyID = env.Get("TEST_S3_KEY")
			cfg.Secret = options.NewSecretString(env.Get("TEST_S3_SECRET"))
			cfg.Prefix = fmt.Sprintf("test-%d", time.Now().UnixNano())
			return cfg, nil
		},

		Factory: s3.NewFactory(),
	}
}

func TestBackendS3(t *testing.T) {
	defer func() {
		if t.Skipped() {
			rtest.SkipDisallowed(t, "vaultic/backend/s3.TestBackendS3")
		}
	}()

	vars := []string{
		"VAULTIC_TEST_S3_KEY",
		"VAULTIC_TEST_S3_SECRET",
		"VAULTIC_TEST_S3_REPOSITORY",
	}

	for _, v := range vars {
		if os.Getenv(v) == "" {
			t.Skipf("environment variable %v not set", v)
			return
		}
	}

	t.Logf("run tests")
	newS3TestSuite().RunTests(t)
}

func TestBackendBackblaze(t *testing.T) {
	runLiveProviderSuite(t, "backblaze")
}

func TestBackendWasabi(t *testing.T) {
	runLiveProviderSuite(t, "wasabi")
}

func runLiveProviderSuite(t *testing.T, provider string) {
	prefix := "VAULTIC_TEST_" + strings.ToUpper(provider) + "_S3_"
	for _, name := range []string{"KEY", "SECRET", "REPOSITORY"} {
		if os.Getenv(prefix+name) == "" {
			t.Skipf("%s%s is not set", prefix, name)
		}
	}
	if os.Getenv("VAULTIC_TEST_S3_DESTRUCTIVE") != "YES" {
		t.Fatalf("VAULTIC_TEST_S3_DESTRUCTIVE=YES is required for destructive live provider tests")
	}
	cfg, err := s3.ParseConfig(os.Getenv(prefix + "REPOSITORY"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Provider = provider
	cfg.KeyID = os.Getenv(prefix + "KEY")
	cfg.Secret = options.NewSecretString(os.Getenv(prefix + "SECRET"))
	random, _ := newRandomCredentials(t)
	cfg.Prefix = fmt.Sprintf("vaultic-live-%s-%d-%s", provider, time.Now().UnixNano(), random)
	suite := &test.Suite[s3.Config]{MinimalData: true, Config: cfg, NewConfig: func() (*s3.Config, error) {
		return cfg, nil
	}, Factory: s3.NewFactory()}
	probeSuiteCapabilities(t, suite, false)
	suite.RunTests(t)
}

func BenchmarkBackendS3(t *testing.B) {
	vars := []string{
		"VAULTIC_TEST_S3_KEY",
		"VAULTIC_TEST_S3_SECRET",
		"VAULTIC_TEST_S3_REPOSITORY",
	}

	for _, v := range vars {
		if os.Getenv(v) == "" {
			t.Skipf("environment variable %v not set", v)
			return
		}
	}

	t.Logf("run tests")
	newS3TestSuite().RunBenchmarks(t)
}
