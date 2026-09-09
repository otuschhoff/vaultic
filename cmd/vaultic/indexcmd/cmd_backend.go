package indexcmd

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sort"
	"time"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/backend/gdrive"
	vaulticerrors "github.com/otuschhoff/vaultic/internal/errors"
	"github.com/otuschhoff/vaultic/internal/global"
	indexbroker "github.com/otuschhoff/vaultic/internal/index/broker"
	"github.com/otuschhoff/vaultic/internal/topology"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
	"github.com/spf13/cobra"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"
)

func NewBackendCommand(globalOptions *global.Options) *cobra.Command {
	command := &cobra.Command{Use: "backend", Short: "Enroll and verify storage backends", Args: cobra.NoArgs, DisableAutoGenTag: true}
	command.AddCommand(newBackendEnrollCommand(globalOptions), newBackendVerifyCommand(globalOptions))
	return command
}

func newBackendEnrollCommand(globalOptions *global.Options) *cobra.Command {
	command := &cobra.Command{Use: "enroll", Short: "Enroll a backend credential into sealed topology", Args: cobra.NoArgs, DisableAutoGenTag: true}
	command.AddCommand(newBackendEnrollGDriveCommand(globalOptions))
	return command
}

func newBackendEnrollGDriveCommand(globalOptions *global.Options) *cobra.Command {
	var clientID, clientSecretFile, credentialRef, capsulePath, capsuleDirectory string
	var backendID, driveID, rootFolderID, drivePath, role, failureDomain string
	var offsite bool
	var memberSpecs, externalMemberFiles []string
	command := &cobra.Command{
		Use: "gdrive", Short: "Enroll Google Drive OAuth without storing or printing its refresh token", Args: cobra.NoArgs, DisableAutoGenTag: true,
		RunE: func(command *cobra.Command, _ []string) error {
			secret, err := readProtectedBinary(clientSecretFile, "Google OAuth client secret", false)
			if err != nil {
				return err
			}
			defer clear(secret)
			credential, err := enrollGDrive(command.Context(), globalOptions, clientID, string(secret))
			if err != nil {
				return err
			}
			defer func() { credential = topology.Credential{} }()
			capsule, err := indexbroker.LoadCapsule(capsulePath)
			if err != nil {
				return err
			}
			policy, err := capsule.PolicyDefinition()
			if err != nil {
				return err
			}
			members, credentials, err := parseOfflinePolicyMembers(memberSpecs)
			if err != nil {
				return err
			}
			externalMembers, tokens, err := parseExternalPolicyMembers(command.Context(), capsule.RepositoryID(), externalMemberFiles)
			defer clearPolicyCredentials(credentials)
			defer clearPolicyCredentials(tokens)
			if err != nil {
				return err
			}
			if err := validatePolicyMemberCredentials(policy, members, externalMembers); err != nil {
				return err
			}
			client, err := indexbroker.Dial(command.Context(), globalOptions.KeyBrokerSocket)
			if err != nil {
				return err
			}
			status, err := client.Status(command.Context())
			if err != nil {
				_ = client.Close() // Preserve the status error; connection cleanup is best effort.
				return err
			}
			if err := matchQuorumCapsule(capsule.RepositoryID(), capsule.Generation(), capsule.LogicalID(), capsule.PolicyHash(), status); err != nil {
				_ = client.Close() // Preserve the capsule mismatch; connection cleanup is best effort.
				return err
			}
			backend := topology.PackBackend{
				ID: backendID, Provider: topology.ProviderGoogleDrive,
				Endpoint: map[string]any{"drive_id": driveID, "root_folder_id": rootFolderID, "path": drivePath},
				Role:     topology.BackendRole(role), Offsite: offsite,
				FailureDomain: failureDomain,
				CredentialPolicy: &topology.CredentialPolicy{Static: &topology.StaticCredentialPolicy{
					Generation: 1,
					Bindings:   topology.CredentialBindings{StorageMaintain: credentialRef},
				}},
			}
			mutation := topology.Mutation{
				Operation: "set-backend-policy", Backend: &backend,
				Credentials: map[string]topology.Credential{credentialRef: credential},
			}
			prepared, err := client.PrepareTopologyMutation(
				command.Context(), globalOptions.KeyBrokerReleaseManifest, mutation, members, externalMembers,
			)
			vaulticerrors.LogClose(client, "close key broker after Google Drive enrollment", func(string, ...any) {})
			if err != nil {
				return err
			}
			options := &indexKeysOptions{RepositoryID: capsule.RepositoryID()}
			return publishAndActivatePolicyMutation(command.Context(), globalOptions, options, "enroll-gdrive", capsuleDirectory, status, prepared)
		},
	}
	command.Flags().StringVar(&clientID, "client-id", "", "Google OAuth installed-app client ID")
	command.Flags().StringVar(&clientSecretFile, "client-secret-file", "", "mode-0600 Google OAuth client secret file")
	command.Flags().StringVar(&credentialRef, "credential-ref", "", "new topology credential reference (cred:NAME)")
	command.Flags().StringVar(&capsulePath, "capsule", "", "current immutable recovery capsule")
	command.Flags().StringVar(&capsuleDirectory, "capsule-directory", "", "deterministic local capsule generation directory")
	command.Flags().StringVar(&backendID, "backend-id", "", "pack backend ID")
	command.Flags().StringVar(&driveID, "drive-id", "", "Google shared-drive ID")
	command.Flags().StringVar(&rootFolderID, "root-folder-id", "root", "Google Drive root folder ID")
	command.Flags().StringVar(&drivePath, "path", "", "repository folder path below the root")
	command.Flags().StringVar(&role, "role", "primary", "placement role")
	command.Flags().StringVar(&failureDomain, "failure-domain", "google-drive", "placement failure domain")
	command.Flags().BoolVar(&offsite, "offsite", true, "count this backend as offsite")
	command.Flags().StringArrayVar(&memberSpecs, "member", nil, "existing member ID=PROVIDER:FILE (repeatable; all members required)")
	command.Flags().StringArrayVar(&externalMemberFiles, "external-member-file", nil, "existing protected external-member definition (repeatable)")
	for _, name := range []string{"client-id", "client-secret-file", "credential-ref", "capsule", "capsule-directory", "backend-id", "drive-id", "path"} {
		mustMarkFlagRequired(command, name)
	}
	return command
}

func enrollGDrive(ctx context.Context, globalOptions *global.Options, clientID, clientSecret string) (topology.Credential, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return topology.Credential{}, err
	}
	defer listener.Close()
	redirect := "http://" + listener.Addr().String() + "/oauth2/callback"
	config := oauth2.Config{
		ClientID: clientID, ClientSecret: clientSecret, Endpoint: google.Endpoint,
		RedirectURL: redirect, Scopes: []string{drive.DriveScope},
	}
	stateBytes := make([]byte, 24)
	if _, err := rand.Read(stateBytes); err != nil {
		return topology.Credential{}, err
	}
	state := base64.RawURLEncoding.EncodeToString(stateBytes)
	type callbackResult struct {
		code string
		err  error
	}
	result := make(chan callbackResult, 1)
	server := &http.Server{ReadHeaderTimeout: 5 * time.Second}
	server.Handler = http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/oauth2/callback" || request.URL.Query().Get("state") != state {
			http.Error(writer, "invalid OAuth callback", http.StatusBadRequest)
			result <- callbackResult{err: fmt.Errorf("invalid OAuth callback")}
			return
		}
		code := request.URL.Query().Get("code")
		if code == "" {
			result <- callbackResult{err: fmt.Errorf("google OAuth did not return an authorization code")}
			return
		}
		// The OAuth result is delivered independently of this courtesy response.
		_, _ = writer.Write([]byte("Google Drive enrollment received. Return to Vaultic."))
		result <- callbackResult{code: code}
	})
	go func() { _ = server.Serve(listener) }() // Shutdown and listener closure are the expected server termination paths.
	defer shutdownOAuthServer(ctx, server)
	globalOptions.Term.Print("Open this URL to authorize Google Drive:\n" + config.AuthCodeURL(state, oauth2.AccessTypeOffline, oauth2.ApprovalForce) + "\n")
	var callback callbackResult
	select {
	case callback = <-result:
	case <-ctx.Done():
		return topology.Credential{}, ctx.Err()
	}
	if callback.err != nil {
		return topology.Credential{}, callback.err
	}
	token, err := config.Exchange(ctx, callback.code)
	if err != nil {
		return topology.Credential{}, fmt.Errorf("exchange Google OAuth code: %w", err)
	}
	if token.RefreshToken == "" {
		return topology.Credential{}, fmt.Errorf("google OAuth response did not include a refresh token")
	}
	return topology.Credential{
		Kind: topology.CredentialOAuth2RefreshToken, ClientID: clientID, ClientSecret: clientSecret,
		RefreshToken: token.RefreshToken, Scopes: []string{drive.DriveScope},
		TokenURI: google.Endpoint.TokenURL, IssuedAt: time.Now().UTC().Format(time.RFC3339),
	}, nil
}

func shutdownOAuthServer(ctx context.Context, server *http.Server) {
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	vaulticerrors.LogCleanup(
		"shut down Google OAuth callback server",
		func() error { return server.Shutdown(shutdownCtx) },
		log.Printf,
	)
}

func newBackendVerifyCommand(globalOptions *global.Options) *cobra.Command {
	var compare []string
	var credentialFile, credentialRef string
	command := &cobra.Command{
		Use: "verify", Short: "Compare backend object inventories", Args: cobra.NoArgs, DisableAutoGenTag: true,
		RunE: func(command *cobra.Command, _ []string) error {
			if len(compare) != 2 {
				return fmt.Errorf("--compare requires exactly two repository locations")
			}
			ctx, cleanup, err := comparisonCredentialContext(
				command.Context(), globalOptions, credentialFile, credentialRef,
			)
			if err != nil {
				return err
			}
			defer cleanup()
			printer := progress.NewTerminalPrinter(globalOptions.JSON, globalOptions.Verbosity, globalOptions.Term)
			left, err := global.OpenBackend(ctx, compare[0], *globalOptions, printer)
			if err != nil {
				return err
			}
			defer left.Close()
			right, err := global.OpenBackend(ctx, compare[1], *globalOptions, printer)
			if err != nil {
				return err
			}
			defer right.Close()
			leftSet, err := backendInventory(ctx, left)
			if err != nil {
				return err
			}
			rightSet, err := backendInventory(ctx, right)
			if err != nil {
				return err
			}
			if len(leftSet) != len(rightSet) {
				return fmt.Errorf("backend inventories differ: %d versus %d objects", len(leftSet), len(rightSet))
			}
			for index := range leftSet {
				if leftSet[index] != rightSet[index] {
					return fmt.Errorf("backend inventories differ at %s", leftSet[index])
				}
			}
			globalOptions.Term.Print(fmt.Sprintf("backend inventories match: %d objects\n", len(leftSet)))
			return nil
		},
	}
	command.Flags().StringSliceVar(&compare, "compare", nil, "two backend locations to compare")
	command.Flags().StringVar(&credentialRef, "credential-ref", "", "broker-leased credential reference for a native gdrive comparison")
	command.Flags().StringVar(&credentialFile, "gdrive-credential-file", "", "mode-0600 credential for a native gdrive comparison")
	return command
}

func comparisonCredentialContext(
	ctx context.Context,
	globalOptions *global.Options,
	credentialFile, credentialRef string,
) (context.Context, func(), error) {
	if credentialFile != "" && credentialRef != "" {
		return ctx, func() {}, fmt.Errorf("--credential-ref and --gdrive-credential-file are mutually exclusive")
	}
	if credentialRef != "" {
		client, err := indexbroker.Dial(ctx, globalOptions.KeyBrokerSocket)
		if err != nil {
			return ctx, func() {}, err
		}
		lease, err := client.AcquireCredentialLease(
			ctx, globalOptions.KeyBrokerReleaseManifest, credentialRef, globalOptions.KeyBrokerLeaseDuration,
		)
		if err != nil {
			vaulticerrors.CloseQuietly(client)
			return ctx, func() {}, err
		}
		var credential topology.Credential
		if err := json.Unmarshal(lease.Key, &credential); err != nil {
			clear(lease.Key)
			vaulticerrors.CloseQuietly(client)
			return ctx, func() {}, fmt.Errorf("decode leased comparison credential: %w", err)
		}
		configured := gdrive.WithCredentials(ctx, topologyGDriveCredentials(credential))
		return configured, func() {
			clear(lease.Key)
			vaulticerrors.CloseQuietly(client)
		}, nil
	}
	if credentialFile != "" {
		var credential topology.Credential
		if err := readProtectedJSON(credentialFile, "Google Drive credential", &credential); err != nil {
			return ctx, func() {}, err
		}
		return gdrive.WithCredentials(ctx, topologyGDriveCredentials(credential)), func() {}, nil
	}
	return ctx, func() {}, nil
}

func topologyGDriveCredentials(credential topology.Credential) gdrive.Credentials {
	return gdrive.Credentials{
		ClientID: credential.ClientID, ClientSecret: credential.ClientSecret,
		RefreshToken: credential.RefreshToken, TokenURI: credential.TokenURI,
		Scopes: credential.Scopes, ServiceAccountJSON: []byte(credential.ServiceAccountJSON), Subject: credential.Subject,
	}
}

func backendInventory(ctx context.Context, store backend.Backend) ([]string, error) {
	var inventory []string
	for fileType := backend.PackFile; fileType <= backend.StagingFile; fileType++ {
		err := store.List(ctx, fileType, func(info backend.FileInfo) error {
			handle := backend.Handle{Type: fileType, Name: info.Name}
			digest := sha256.New()
			var loaded int64
			if err := store.Load(ctx, handle, 0, 0, func(reader io.Reader) error {
				written, err := io.Copy(digest, reader)
				loaded += written
				return err
			}); err != nil {
				return fmt.Errorf("hash %v %q: %w", fileType, info.Name, err)
			}
			if loaded != info.Size {
				return fmt.Errorf("hash %v %q: listed size %d, loaded %d", fileType, info.Name, info.Size, loaded)
			}
			inventory = append(inventory, fmt.Sprintf("%d/%s/%d/%x", fileType, info.Name, info.Size, digest.Sum(nil)))
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	sort.Strings(inventory)
	return inventory, nil
}
