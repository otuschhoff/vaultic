package global

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/pflag"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/backend/all"
	"github.com/otuschhoff/vaultic/internal/errors"
	"github.com/otuschhoff/vaultic/internal/repository/bootstrap"
	"github.com/otuschhoff/vaultic/internal/repository/crypto"
	rtest "github.com/otuschhoff/vaultic/internal/test"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func TestReadRepo(t *testing.T) {
	tempDir := rtest.TempDir(t)

	// test --repo option
	var gopts Options
	gopts.Repo = tempDir
	repo, err := readRepo(gopts)
	rtest.OK(t, err)
	rtest.Equals(t, tempDir, repo)

	// test --repository-file option
	foo := filepath.Join(tempDir, "foo")
	err = os.WriteFile(foo, []byte(tempDir+"\n"), 0666)
	rtest.OK(t, err)

	var gopts2 Options
	gopts2.RepositoryFile = foo
	repo, err = readRepo(gopts2)
	rtest.OK(t, err)
	rtest.Equals(t, tempDir, repo)

	var gopts3 Options
	gopts3.RepositoryFile = foo + "-invalid"
	_, err = readRepo(gopts3)
	if err == nil {
		t.Fatal("must not read repository path from invalid file path")
	}
}

func TestResolveRepositoryLocationRejectsConflictingSources(t *testing.T) {
	_, err := resolveRepositoryLocation(context.Background(), Options{
		BootstrapProfile: "bootstrap.toml",
		Repo:             "repository",
	}, vaultic.NewNoopPrinter())
	if !errors.IsFatal(err) || !strings.Contains(err.Error(), "--bootstrap-profile is mutually exclusive with --repo and --repository-file") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateBrokeredUnlockOptions(t *testing.T) {
	conflictError := "brokered unlock is mutually exclusive with password, direct-key, Azure-secret, and key-in-DB routes"
	tests := []struct {
		name    string
		options Options
		wantErr string
	}{
		{
			name:    "missing release manifest",
			options: Options{KeyBrokerSocket: "broker.sock"},
			wantErr: "--key-broker-release-manifest is required with --key-broker-socket",
		},
		{name: "metadata key in DB", options: Options{MetadataKeyInDB: true}, wantErr: conflictError},
		{name: "direct key", options: Options{MasterKey: "key"}, wantErr: conflictError},
		{name: "direct key file", options: Options{MasterKeyFile: "key-file"}, wantErr: conflictError},
		{name: "direct key command", options: Options{MasterKeyCommand: "key-command"}, wantErr: conflictError},
		{name: "password", options: Options{Password: "password"}, wantErr: conflictError},
		{name: "password file", options: Options{PasswordFile: "password-file"}, wantErr: conflictError},
		{name: "password command", options: Options{PasswordCommand: "password-command"}, wantErr: conflictError},
		{name: "Azure secret", options: Options{AzureKeyVaultURL: "https://example.invalid"}, wantErr: conflictError},
		{name: "insecure no password", options: Options{InsecureNoPassword: true}, wantErr: conflictError},
		{
			name: "valid",
			options: Options{
				KeyBrokerSocket:          "broker.sock",
				KeyBrokerReleaseManifest: "release.json",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.options.KeyBrokerSocket == "" {
				test.options.KeyBrokerSocket = "broker.sock"
				test.options.KeyBrokerReleaseManifest = "release.json"
			}
			err := validateBrokeredUnlockOptions(test.options)
			if test.wantErr == "" {
				rtest.OK(t, err)
				return
			}
			if !errors.IsFatal(err) || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestValidateMetadataLossRecoveryOptions(t *testing.T) {
	tests := []struct {
		name    string
		options Options
		wantErr bool
	}{
		{name: "direct key", options: Options{MetadataLossRecovery: true, MasterKey: "key"}},
		{name: "broker", options: Options{MetadataLossRecovery: true, KeyBrokerSocket: "broker.sock"}},
		{name: "missing key", options: Options{MetadataLossRecovery: true}, wantErr: true},
		{name: "metadata key in DB", options: Options{MetadataLossRecovery: true, MetadataKeyInDB: true}, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateMetadataLossRecoveryOptions(test.options)
			if (err != nil) != test.wantErr {
				t.Fatalf("error = %v, wantErr = %v", err, test.wantErr)
			}
		})
	}
}

func TestResolveBootstrapRepositorySurvivesSeedLoss(t *testing.T) {
	ctx := context.Background()
	tempDir := rtest.TempDir(t)
	vaultic.TestDisableCheckPolynomial(t)
	survivingLocation := filepath.Join(tempDir, "surviving")
	if err := os.Mkdir(survivingLocation, 0o700); err != nil {
		t.Fatal(err)
	}
	key := crypto.NewRandomKey()
	encodedKey, err := json.Marshal(key)
	if err != nil {
		t.Fatal(err)
	}
	masterKey := base64.StdEncoding.EncodeToString(encodedKey)
	manifest := bootstrap.Manifest{
		Format: 1, RepositoryID: "repo-a", Generation: 1, CreatedAt: time.Now().UTC(),
		Backends: []vaultic.PlacementBackend{{ID: "surviving", Location: survivingLocation, FailureDomain: "site-a"}},
		Policy:   vaultic.PlacementPolicy{MinCopies: 1, MinDomains: 1}, StagingBackends: []string{"surviving"},
	}
	projection := vaultic.Config{Version: 2, ID: "repo-a", PlacementBackends: manifest.Backends, PlacementPolicy: manifest.Policy, StagingBackends: manifest.StagingBackends}
	manifest.RepositoryConfig, err = json.Marshal(projection)
	if err != nil {
		t.Fatal(err)
	}
	projectionDigest := sha256.Sum256(manifest.RepositoryConfig)
	manifest.ConfigSHA256 = fmt.Sprintf("%x", projectionDigest)
	sealed, digest, err := bootstrap.Seal(manifest, key.EncryptionKey[:])
	if err != nil {
		t.Fatal(err)
	}
	gopts := Options{Backends: all.Backends(), MasterKey: masterKey}
	printer := vaultic.NewNoopPrinter()
	seedBackend, err := innerOpenBackend(ctx, survivingLocation, gopts, nil, false, printer)
	if err != nil {
		t.Fatal(err)
	}
	if err := bootstrap.Publish(ctx, map[string]backend.Backend{"surviving": seedBackend}, 1, sealed); err != nil {
		t.Fatal(err)
	}
	if err := seedBackend.Close(); err != nil {
		t.Fatal(err)
	}
	anchorPath := filepath.Join(tempDir, "anchor.json")
	profilePath := filepath.Join(tempDir, "bootstrap.toml")
	profile := "format = 1\nrepository_id = \"repo-a\"\nanchor_file = \"" + anchorPath + "\"\n" +
		"[[seed]]\nid = \"missing\"\nlocation = \"" + filepath.Join(tempDir, "missing") + "\"\n" +
		"[[seed]]\nid = \"surviving\"\nlocation = \"" + survivingLocation + "\"\n"
	if err := os.WriteFile(profilePath, []byte(profile), 0o600); err != nil {
		t.Fatal(err)
	}
	gopts.BootstrapProfile = profilePath
	location, resolvedKey, brokerClient, err := resolveBootstrapRepository(ctx, gopts, printer)
	if err != nil {
		t.Fatal(err)
	}
	if location != survivingLocation || resolvedKey != masterKey || brokerClient != nil {
		t.Fatalf("resolution = %q, key=%t, broker=%v", location, resolvedKey == masterKey, brokerClient)
	}
	anchor, err := bootstrap.LoadAnchor(anchorPath)
	if err != nil {
		t.Fatal(err)
	}
	if anchor.RepositoryID != "repo-a" || anchor.Generation != 1 || anchor.SHA256 != digest {
		t.Fatalf("anchor = %#v", anchor)
	}
	dataPlaneRepo, err := OpenDataPlaneRepository(ctx, gopts, printer)
	if err != nil {
		t.Fatal(err)
	}
	defer dataPlaneRepo.Close()
	if dataPlaneRepo.Config().ID != "repo-a" {
		t.Fatalf("data-plane repository config = %#v", dataPlaneRepo.Config())
	}
	if _, _, err := dataPlaneRepo.DeferredUploadPlan(); err != nil {
		t.Fatal(err)
	}
}

func TestReadEmptyPassword(t *testing.T) {
	opts := Options{InsecureNoPassword: true}
	password, err := readPassword(context.TODO(), opts, "test")
	rtest.OK(t, err)
	rtest.Equals(t, "", password, "got unexpected password")

	opts.Password = "invalid"
	_, err = readPassword(context.TODO(), opts, "test")
	rtest.Assert(t, strings.Contains(err.Error(), "must not be specified together with providing a password via a cli option or environment variable"), "unexpected error message, got %v", err)
}

func TestAzureKeyVaultPasswordSourceValidation(t *testing.T) {
	for _, opts := range []Options{
		{AzureKeyVaultURL: "https://example.vault.azure.net"},
		{AzureKeyVaultSecret: "vaultic"},
		{AzureKeyVaultURL: "https://example.vault.azure.net", AzureKeyVaultSecret: "vaultic", PasswordFile: "password"},
		{AzureKeyVaultURL: "https://example.vault.azure.net", AzureKeyVaultSecret: "vaultic", AzureKeyVaultTimeout: -1},
	} {
		if _, err := resolvePassword(&opts, "VAULTIC_PASSWORD"); err == nil {
			t.Fatalf("expected invalid Key Vault options %#v to fail", opts)
		}
	}
}

func TestPackSizeEnvParseError(t *testing.T) {
	t.Setenv("VAULTIC_PACK_SIZE", "64MiB")

	var gopts Options
	gopts.AddFlags(pflag.NewFlagSet("test", pflag.ContinueOnError))

	err := gopts.PreRun(false)
	rtest.Assert(t, err != nil, "expected error for invalid pack size env")
	rtest.Assert(t, errors.IsFatal(err), "expected fatal error for invalid pack size env, got %T", err)
	rtest.Assert(t, strings.Contains(err.Error(), "VAULTIC_PACK_SIZE"), "error should mention VAULTIC_PACK_SIZE, got %v", err)
}

func TestPackSizeEnvApplied(t *testing.T) {
	t.Setenv("VAULTIC_PACK_SIZE", "64")

	var gopts Options
	gopts.AddFlags(pflag.NewFlagSet("test", pflag.ContinueOnError))

	err := gopts.PreRun(false)
	rtest.OK(t, err)
	rtest.Equals(t, uint(64), gopts.PackSize)
}

func TestPackSizeLegacyEnvFallback(t *testing.T) {
	// the legacy RESTIC_* variable is still honored
	t.Setenv("RESTIC_PACK_SIZE", "128")

	var gopts Options
	gopts.AddFlags(pflag.NewFlagSet("test", pflag.ContinueOnError))

	err := gopts.PreRun(false)
	rtest.OK(t, err)
	rtest.Equals(t, uint(128), gopts.PackSize)
}

func TestPackSizeEnvPrimaryWinsOverLegacy(t *testing.T) {
	t.Setenv("VAULTIC_PACK_SIZE", "64")
	t.Setenv("RESTIC_PACK_SIZE", "128")

	var gopts Options
	gopts.AddFlags(pflag.NewFlagSet("test", pflag.ContinueOnError))

	err := gopts.PreRun(false)
	rtest.OK(t, err)
	rtest.Equals(t, uint(64), gopts.PackSize)
}

func TestPackSizeEnvIgnoredWhenFlagSet(t *testing.T) {
	t.Setenv("VAULTIC_PACK_SIZE", "64MiB")

	var gopts Options
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	gopts.AddFlags(fs)

	err := fs.Set("pack-size", "64")
	rtest.OK(t, err)

	err = gopts.PreRun(false)
	rtest.OK(t, err)
	rtest.Equals(t, uint(64), gopts.PackSize)
}

func TestCompressionEnvParseError(t *testing.T) {
	t.Setenv("VAULTIC_COMPRESSION", "invalid")

	var gopts Options
	gopts.AddFlags(pflag.NewFlagSet("test", pflag.ContinueOnError))

	err := gopts.PreRun(false)
	rtest.Assert(t, err != nil, "expected error for invalid compression env")
	rtest.Assert(t, errors.IsFatal(err), "expected fatal error for invalid compression env, got %T", err)
	rtest.Assert(t, strings.Contains(err.Error(), "VAULTIC_COMPRESSION"), "error should mention VAULTIC_COMPRESSION, got %v", err)
}

func TestCompressionEnvApplied(t *testing.T) {
	t.Setenv("VAULTIC_COMPRESSION", "max")

	var gopts Options
	gopts.AddFlags(pflag.NewFlagSet("test", pflag.ContinueOnError))

	err := gopts.PreRun(false)
	rtest.OK(t, err)
	rtest.Equals(t, "max", gopts.Compression.String())
}

func TestCompressionEnvIgnoredWhenFlagSet(t *testing.T) {
	t.Setenv("VAULTIC_COMPRESSION", "invalid")

	var gopts Options
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	gopts.AddFlags(fs)

	err := fs.Set("compression", "off")
	rtest.OK(t, err)

	err = gopts.PreRun(false)
	rtest.OK(t, err)
	rtest.Equals(t, "off", gopts.Compression.String())
}
