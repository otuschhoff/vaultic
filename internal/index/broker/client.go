// Package broker implements the local Vaultic key-broker protocol.
package broker

import (
	"bufio"
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/cloudflare/circl/hpke"
	"golang.org/x/crypto/argon2"
	"golang.org/x/crypto/hkdf"
)

const (
	protocolVersion = "vaultic-key-broker.v1"
	sessionInfo     = "vaultic-key-broker-contribution-v1"
	maxResponse     = 1024 * 1024
)

type Client struct {
	connection net.Conn
	reader     *bufio.Reader
	protocol   string
	challenge  string
}

type Status struct {
	Protocol          string   `json:"protocol"`
	Locked            bool     `json:"locked"`
	RepositoryID      string   `json:"repository_id"`
	CapsuleGeneration uint64   `json:"capsule_generation"`
	CapsuleLogicalID  string   `json:"capsule_logical_id"`
	PolicyHash        string   `json:"policy_hash"`
	EpochID           *string  `json:"epoch_id"`
	ActiveSessions    int      `json:"active_sessions"`
	ActiveLeases      int      `json:"active_leases"`
	MinimumCustodians int      `json:"minimum_custodians"`
	PrincipalVerified bool     `json:"principal_verified"`
	HardwareVerified  bool     `json:"hardware_verified"`
	CustodyAssumed    bool     `json:"custody_assumed"`
	Compliant         bool     `json:"compliant"`
	Findings          []string `json:"findings"`
}

type ReleaseManifest struct {
	Component        string `json:"component"`
	Version          uint64 `json:"version"`
	ReleaseIdentity  string `json:"release_identity"`
	ExecutableSHA256 string `json:"executable_sha256"`
	Signature        string `json:"signature"`
}

type Lease struct {
	LeaseID           string `json:"lease_id"`
	EpochID           string `json:"epoch_id"`
	Capability        string `json:"capability"`
	ExpiresUnixMS     uint64 `json:"expires_unix_ms"`
	KeyVersion        uint32 `json:"key_version"`
	CapsuleGeneration uint64 `json:"capsule_generation"`
	Key               []byte `json:"-"`
}

type SessionTranscript struct {
	Protocol          string `json:"protocol"`
	SessionID         string `json:"session_id"`
	RepositoryID      string `json:"repository_id"`
	CapsuleGeneration uint64 `json:"capsule_generation"`
	EndpointBinding   string `json:"endpoint_binding"`
	Nonce             string `json:"nonce"`
	ExpiresUnixMS     uint64 `json:"expires_unix_ms"`
	HPKEPublicKey     string `json:"hpke_public_key"`
}

type SignedSession struct {
	Transcript  SessionTranscript `json:"transcript"`
	Signature   string            `json:"signature"`
	Fingerprint string            `json:"fingerprint"`
}

type EncryptedContribution struct {
	SessionID   string `json:"session_id"`
	EncappedKey string `json:"encapped_key"`
	Ciphertext  string `json:"ciphertext"`
	Tag         string `json:"tag"`
}

type capsule struct {
	Header              capsuleHeader   `json:"header"`
	Policy              json.RawMessage `json:"policy"`
	Members             []memberShare   `json:"members"`
	MetadataDEK         json.RawMessage `json:"metadata_dek"`
	RepositoryMasterKey json.RawMessage `json:"repository_master_key"`
}

type capsuleHeader struct {
	Format                  uint32 `json:"format"`
	LogicalID               string `json:"logical_id"`
	RepositoryID            string `json:"repository_id"`
	Generation              uint64 `json:"generation"`
	RootKeyVersion          uint32 `json:"root_key_version"`
	MetadataDEKVersion      uint32 `json:"metadata_dek_version"`
	RepositoryKeyVersion    uint32 `json:"repository_key_version"`
	Algorithm               string `json:"algorithm"`
	PolicyHash              string `json:"policy_hash"`
	BrokerIdentityPublicKey string `json:"broker_identity_public_key"`
	PolicyIntent            string `json:"policy_intent"`
}

type memberShare struct {
	MemberID     string            `json:"member_id"`
	GroupID      string            `json:"group_id"`
	ShareIndex   uint8             `json:"share_index"`
	Threshold    uint8             `json:"threshold"`
	ShareCount   uint8             `json:"share_count"`
	Provider     string            `json:"provider"`
	KeyReference string            `json:"key_reference"`
	WrappedShare string            `json:"wrapped_share"`
	Nonce        *string           `json:"nonce"`
	Argon2       *argon2Config     `json:"argon2"`
	Principal    *principalBinding `json:"principal"`
	Hardware     *hardwareBinding  `json:"hardware"`
}

type principalBinding struct {
	Authority              string `json:"authority"`
	TenantAccountOrProject string `json:"tenant_account_or_project"`
	ImmutablePrincipalID   string `json:"immutable_principal_id"`
}

type hardwareBinding struct {
	CredentialID           string  `json:"credential_id"`
	PublicKey              string  `json:"public_key"`
	SerialNumber           *string `json:"serial_number"`
	AttestationFingerprint *string `json:"attestation_fingerprint"`
	UserPresenceRequired   bool    `json:"user_presence_required"`
}

type ExternalMemberContext struct {
	RepositoryID   string
	Generation     uint64
	RootKeyVersion uint32
	PolicyHash     string
	MemberID       string
	Provider       string
	KeyReference   string
	Purpose        string
}

type VerifiedPrincipal struct {
	Authority              string
	TenantAccountOrProject string
	ImmutablePrincipalID   string
}

type ExternalMemberUnwrapper interface {
	UnwrapMember(context.Context, ExternalMemberContext, []byte) ([]byte, VerifiedPrincipal, error)
}

type argon2Config struct {
	Salt        string `json:"salt"`
	MemoryKiB   uint32 `json:"memory_kib"`
	Iterations  uint32 `json:"iterations"`
	Parallelism uint32 `json:"parallelism"`
}

type responseEnvelope struct {
	Result            string         `json:"result"`
	Code              string         `json:"code"`
	Message           string         `json:"message"`
	Protocol          string         `json:"protocol"`
	Challenge         string         `json:"challenge"`
	Locked            bool           `json:"locked"`
	RepositoryID      string         `json:"repository_id"`
	CapsuleGeneration uint64         `json:"capsule_generation"`
	EpochID           *string        `json:"epoch_id"`
	ActiveSessions    int            `json:"active_sessions"`
	ActiveLeases      int            `json:"active_leases"`
	MinimumCustodians int            `json:"minimum_custodians"`
	PrincipalVerified bool           `json:"principal_verified"`
	HardwareVerified  bool           `json:"hardware_verified"`
	CustodyAssumed    bool           `json:"custody_assumed"`
	Compliant         bool           `json:"compliant"`
	Findings          []string       `json:"findings"`
	Session           *SignedSession `json:"session"`
	Unlocked          bool           `json:"unlocked"`
	LeaseID           string         `json:"lease_id"`
	Capability        string         `json:"capability"`
	ExpiresUnixMS     uint64         `json:"expires_unix_ms"`
	KeyVersion        uint32         `json:"key_version"`
	Key               string         `json:"key"`
}

func Dial(ctx context.Context, socket string) (*Client, error) {
	if socket == "" {
		return nil, errors.New("broker socket is required")
	}
	connection, err := (&net.Dialer{}).DialContext(ctx, "unix", socket)
	if err != nil {
		return nil, fmt.Errorf("connect key broker: %w", err)
	}
	client := &Client{connection: connection, reader: bufio.NewReaderSize(connection, maxResponse)}
	var response responseEnvelope
	if err := client.call(ctx, map[string]any{"operation": "negotiate", "protocols": []string{protocolVersion}}, &response); err != nil {
		_ = connection.Close()
		return nil, fmt.Errorf("negotiate key broker protocol: %w", err)
	}
	if response.Result != "negotiated" || response.Protocol != protocolVersion || response.Challenge == "" {
		_ = connection.Close()
		return nil, errors.New("key broker returned an invalid protocol negotiation")
	}
	client.protocol = response.Protocol
	client.challenge = response.Challenge
	return client, nil
}

func (client *Client) Close() error { return client.connection.Close() }

func (client *Client) Status(ctx context.Context) (Status, error) {
	var response responseEnvelope
	if err := client.call(ctx, map[string]any{"operation": "status"}, &response); err != nil {
		return Status{}, err
	}
	if response.Result != "status" || response.Protocol != protocolVersion {
		return Status{}, fmt.Errorf("unexpected broker status response or protocol %q", response.Protocol)
	}
	return Status{Protocol: response.Protocol, Locked: response.Locked, RepositoryID: response.RepositoryID, CapsuleGeneration: response.CapsuleGeneration, EpochID: response.EpochID, ActiveSessions: response.ActiveSessions, ActiveLeases: response.ActiveLeases, MinimumCustodians: response.MinimumCustodians, PrincipalVerified: response.PrincipalVerified, HardwareVerified: response.HardwareVerified, CustodyAssumed: response.CustodyAssumed, Compliant: response.Compliant, Findings: response.Findings}, nil
}

func (client *Client) CreateSession(ctx context.Context, ttl time.Duration) (SignedSession, error) {
	if ttl <= 0 || ttl%time.Second != 0 {
		return SignedSession{}, errors.New("session lifetime must be positive whole seconds")
	}
	var response responseEnvelope
	if err := client.call(ctx, map[string]any{"operation": "create_session", "ttl_seconds": uint64(ttl / time.Second)}, &response); err != nil {
		return SignedSession{}, err
	}
	if response.Result != "session" || response.Session == nil {
		return SignedSession{}, errors.New("unexpected broker session response")
	}
	return *response.Session, nil
}

func (client *Client) SubmitContribution(ctx context.Context, contribution EncryptedContribution) (bool, error) {
	var response responseEnvelope
	if err := client.call(ctx, map[string]any{"operation": "submit_contribution", "contribution": contribution}, &response); err != nil {
		return false, err
	}
	if response.Result != "contribution" {
		return false, errors.New("unexpected broker contribution response")
	}
	return response.Unlocked, nil
}

func (client *Client) Lock(ctx context.Context) error {
	var response responseEnvelope
	if err := client.call(ctx, map[string]any{"operation": "lock"}, &response); err != nil {
		return err
	}
	if response.Result != "ok" {
		return errors.New("unexpected broker lock response")
	}
	return nil
}

func (client *Client) AcquireLease(ctx context.Context, manifestPath, capability string, ttl time.Duration) (Lease, error) {
	if capability != "repository-master-key" && capability != "metadata-loss-recovery" {
		return Lease{}, fmt.Errorf("unsupported Vaultic broker capability %q", capability)
	}
	if ttl <= 0 || ttl > time.Hour || ttl%time.Second != 0 {
		return Lease{}, errors.New("broker lease lifetime must be positive whole seconds and at most one hour")
	}
	manifestData, err := os.ReadFile(manifestPath)
	if err != nil {
		return Lease{}, fmt.Errorf("read release manifest: %w", err)
	}
	var manifest ReleaseManifest
	decoder := json.NewDecoder(bytes.NewReader(manifestData))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return Lease{}, fmt.Errorf("decode release manifest: %w", err)
	}
	if manifest.Component != "vaultic" || manifest.Signature == "" || manifest.ReleaseIdentity == "" {
		return Lease{}, errors.New("release manifest is not a signed Vaultic manifest")
	}
	executable, err := os.Executable()
	if err != nil {
		return Lease{}, err
	}
	executableData, err := os.ReadFile(executable)
	if err != nil {
		return Lease{}, fmt.Errorf("hash running Vaultic executable: %w", err)
	}
	digest := sha256.Sum256(executableData)
	executableDigest := hex.EncodeToString(digest[:])
	if !strings.EqualFold(manifest.ExecutableSHA256, executableDigest) {
		return Lease{}, errors.New("release manifest executable digest does not match running Vaultic")
	}
	if client.protocol != protocolVersion || client.challenge == "" {
		return Lease{}, errors.New("broker lease challenge is unavailable or already consumed")
	}
	challengeDigest := sha256.Sum256([]byte("vaultic-broker-lease-challenge-v1\x00" + protocolVersion + "\x00" + client.challenge + "\x00" + executableDigest))
	request := map[string]any{"operation": "acquire_lease", "component": manifest.Component, "version": manifest.Version, "release_identity": manifest.ReleaseIdentity, "release_signature": manifest.Signature, "capability": capability, "ttl_seconds": uint64(ttl / time.Second), "challenge_response": hex.EncodeToString(challengeDigest[:])}
	client.challenge = ""
	var response responseEnvelope
	if err := client.call(ctx, request, &response); err != nil {
		return Lease{}, err
	}
	if response.Result != "lease" || response.LeaseID == "" || response.EpochID == nil || *response.EpochID == "" || response.KeyVersion == 0 {
		return Lease{}, errors.New("invalid broker lease response")
	}
	key, err := base64.StdEncoding.DecodeString(response.Key)
	if err != nil || len(key) == 0 {
		return Lease{}, errors.New("invalid broker repository key")
	}
	return Lease{LeaseID: response.LeaseID, EpochID: *response.EpochID, Capability: response.Capability, ExpiresUnixMS: response.ExpiresUnixMS, KeyVersion: response.KeyVersion, CapsuleGeneration: response.CapsuleGeneration, Key: key}, nil
}

func (client *Client) call(ctx context.Context, request any, response *responseEnvelope) error {
	deadline, hasDeadline := ctx.Deadline()
	if hasDeadline {
		if err := client.connection.SetDeadline(deadline); err != nil {
			return err
		}
		defer client.connection.SetDeadline(time.Time{})
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	if _, err := client.connection.Write(encoded); err != nil {
		return fmt.Errorf("write broker request: %w", err)
	}
	line, err := client.reader.ReadBytes('\n')
	if err != nil {
		return fmt.Errorf("read broker response: %w", err)
	}
	if len(line) > maxResponse {
		return errors.New("broker response exceeds size limit")
	}
	if err := json.Unmarshal(line, response); err != nil {
		return fmt.Errorf("decode broker response: %w", err)
	}
	if response.Result == "error" {
		return fmt.Errorf("key broker rejected request (%s): %s", response.Code, response.Message)
	}
	return nil
}

func LoadCapsule(path string) (*capsule, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read recovery capsule: %w", err)
	}
	var value capsule
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return nil, fmt.Errorf("decode recovery capsule: %w", err)
	}
	if value.Header.Format != 2 || value.Header.RepositoryID == "" || value.Header.Generation == 0 || len(value.Policy) == 0 || len(value.MetadataDEK) == 0 || len(value.RepositoryMasterKey) == 0 {
		return nil, errors.New("invalid recovery capsule header")
	}
	return &value, nil
}

func (value *capsule) RepositoryID() string { return value.Header.RepositoryID }

func (value *capsule) Generation() uint64 { return value.Header.Generation }

func (value *capsule) LogicalID() string { return value.Header.LogicalID }

func (value *capsule) PolicyHash() string { return value.Header.PolicyHash }

func (value *capsule) ContributeOffline(session SignedSession, endpoint, memberID string, credential []byte, keyfile bool, lastSeenGeneration uint64, now time.Time) (EncryptedContribution, error) {
	if err := value.verifySession(session, endpoint, now); err != nil {
		return EncryptedContribution{}, err
	}
	var member *memberShare
	for index := range value.Members {
		if value.Members[index].MemberID == memberID {
			member = &value.Members[index]
			break
		}
	}
	if member == nil {
		return EncryptedContribution{}, fmt.Errorf("capsule has no member %q", memberID)
	}
	share, err := value.unwrapMember(*member, credential, keyfile)
	if err != nil {
		return EncryptedContribution{}, err
	}
	defer clear(share)
	return value.encryptContribution(session, *member, share, lastSeenGeneration, nil)
}

func (value *capsule) ContributeExternal(ctx context.Context, session SignedSession, endpoint, memberID string, unwrapper ExternalMemberUnwrapper, lastSeenGeneration uint64, now time.Time) (EncryptedContribution, error) {
	if err := value.verifySession(session, endpoint, now); err != nil {
		return EncryptedContribution{}, err
	}
	var member *memberShare
	for index := range value.Members {
		if value.Members[index].MemberID == memberID {
			member = &value.Members[index]
			break
		}
	}
	if member == nil {
		return EncryptedContribution{}, fmt.Errorf("capsule has no member %q", memberID)
	}
	if member.Principal == nil || member.Provider == "offline-argon2id" || member.Provider == "offline-keyfile" {
		return EncryptedContribution{}, errors.New("member is not a principal-bound external provider")
	}
	purpose, err := value.externalSharePurpose(*member)
	if err != nil {
		return EncryptedContribution{}, err
	}
	wrapper, err := base64.StdEncoding.DecodeString(member.WrappedShare)
	if err != nil {
		return EncryptedContribution{}, fmt.Errorf("decode externally wrapped member share: %w", err)
	}
	payload, principal, err := unwrapper.UnwrapMember(ctx, ExternalMemberContext{RepositoryID: value.Header.RepositoryID, Generation: value.Header.Generation, RootKeyVersion: value.Header.RootKeyVersion, PolicyHash: value.Header.PolicyHash, MemberID: member.MemberID, Provider: member.Provider, KeyReference: member.KeyReference, Purpose: purpose}, wrapper)
	if err != nil {
		return EncryptedContribution{}, fmt.Errorf("unwrap external member: %w", err)
	}
	defer clear(payload)
	if principal.Authority != member.Principal.Authority || principal.TenantAccountOrProject != member.Principal.TenantAccountOrProject || principal.ImmutablePrincipalID != member.Principal.ImmutablePrincipalID {
		return EncryptedContribution{}, errors.New("provider-authenticated principal does not match capsule member")
	}
	share, err := decodeExternalShare(purpose, payload)
	if err != nil {
		return EncryptedContribution{}, err
	}
	defer clear(share)
	return value.encryptContribution(session, *member, share, lastSeenGeneration, &principal.ImmutablePrincipalID)
}

func (value *capsule) encryptContribution(session SignedSession, member memberShare, share []byte, lastSeenGeneration uint64, principalID *string) (EncryptedContribution, error) {
	payload, err := json.Marshal(struct {
		MemberID           string  `json:"member_id"`
		ShareIndex         uint8   `json:"share_index"`
		Share              []byte  `json:"share"`
		LastSeenGeneration uint64  `json:"last_seen_generation"`
		PrincipalID        *string `json:"principal_id"`
	}{MemberID: member.MemberID, ShareIndex: member.ShareIndex, Share: share, LastSeenGeneration: lastSeenGeneration, PrincipalID: principalID})
	if err != nil {
		return EncryptedContribution{}, err
	}
	defer clear(payload)
	transcript, err := json.Marshal(session.Transcript)
	if err != nil {
		return EncryptedContribution{}, err
	}
	publicKeyBytes, err := base64.StdEncoding.DecodeString(session.Transcript.HPKEPublicKey)
	if err != nil {
		return EncryptedContribution{}, fmt.Errorf("decode session HPKE public key: %w", err)
	}
	suite := hpke.NewSuite(hpke.KEM_X25519_HKDF_SHA256, hpke.KDF_HKDF_SHA256, hpke.AEAD_ChaCha20Poly1305)
	publicKey, err := hpke.KEM_X25519_HKDF_SHA256.Scheme().UnmarshalBinaryPublicKey(publicKeyBytes)
	if err != nil {
		return EncryptedContribution{}, fmt.Errorf("decode session HPKE public key: %w", err)
	}
	sender, err := suite.NewSender(publicKey, []byte(sessionInfo))
	if err != nil {
		return EncryptedContribution{}, err
	}
	encapped, sealer, err := sender.Setup(rand.Reader)
	if err != nil {
		return EncryptedContribution{}, fmt.Errorf("initialize contribution HPKE: %w", err)
	}
	sealed, err := sealer.Seal(payload, transcript)
	if err != nil {
		return EncryptedContribution{}, fmt.Errorf("encrypt contribution: %w", err)
	}
	if len(sealed) < 16 {
		return EncryptedContribution{}, errors.New("HPKE returned a truncated ciphertext")
	}
	ciphertext, tag := sealed[:len(sealed)-16], sealed[len(sealed)-16:]
	return EncryptedContribution{SessionID: session.Transcript.SessionID, EncappedKey: base64.StdEncoding.EncodeToString(encapped), Ciphertext: base64.StdEncoding.EncodeToString(ciphertext), Tag: base64.StdEncoding.EncodeToString(tag)}, nil
}

func (value *capsule) externalSharePurpose(member memberShare) (string, error) {
	binding, err := json.Marshal([]any{"vaultic-recovery-capsule-external-share", value.Header.RepositoryID, value.Header.Generation, value.Header.RootKeyVersion, value.Header.PolicyHash, member.GroupID, member.MemberID, member.ShareIndex, member.Threshold, member.ShareCount, member.Provider, member.KeyReference, member.Principal, member.Hardware})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(binding)
	return "recovery-capsule-share:" + hex.EncodeToString(digest[:]), nil
}

func decodeExternalShare(purpose string, payload []byte) ([]byte, error) {
	const magic = "VLTCAPSH1"
	digest := sha256.Sum256([]byte(purpose))
	prefix := len(magic) + len(digest)
	if len(payload) <= prefix || string(payload[:len(magic)]) != magic || !bytes.Equal(payload[len(magic):prefix], digest[:]) {
		return nil, errors.New("externally wrapped member share context mismatch")
	}
	return append([]byte(nil), payload[prefix:]...), nil
}

func (value *capsule) verifySession(session SignedSession, endpoint string, now time.Time) error {
	transcript := session.Transcript
	if transcript.Protocol != protocolVersion || transcript.RepositoryID != value.Header.RepositoryID || transcript.CapsuleGeneration != value.Header.Generation || transcript.EndpointBinding != endpoint || transcript.ExpiresUnixMS <= uint64(now.UnixMilli()) || transcript.SessionID == "" {
		return errors.New("unlock session transcript does not match capsule or endpoint")
	}
	encoded, err := json.Marshal(transcript)
	if err != nil {
		return err
	}
	publicKey, err := base64.StdEncoding.DecodeString(value.Header.BrokerIdentityPublicKey)
	if err != nil || len(publicKey) != ed25519.PublicKeySize {
		return errors.New("invalid pinned broker identity public key")
	}
	signature, err := base64.StdEncoding.DecodeString(session.Signature)
	if err != nil || !ed25519.Verify(publicKey, encoded, signature) {
		return errors.New("unlock session signature verification failed")
	}
	digest := sha256.Sum256(encoded)
	parts := make([]string, 0, 5)
	for offset := 0; offset < 10; offset += 2 {
		parts = append(parts, strings.ToUpper(hex.EncodeToString(digest[offset:offset+2])))
	}
	if session.Fingerprint != strings.Join(parts, "-") {
		return errors.New("unlock session fingerprint mismatch")
	}
	return nil
}

func (value *capsule) unwrapMember(member memberShare, credential []byte, keyfile bool) ([]byte, error) {
	var key []byte
	if member.Provider == "offline-argon2id" && !keyfile {
		if member.Argon2 == nil || member.Argon2.MemoryKiB < 64*1024 || member.Argon2.Iterations < 3 || member.Argon2.Parallelism == 0 {
			return nil, errors.New("missing or weak Argon2id member parameters")
		}
		salt, err := base64.StdEncoding.DecodeString(member.Argon2.Salt)
		if err != nil || len(salt) != 16 {
			return nil, errors.New("invalid Argon2id salt")
		}
		key = argon2.IDKey(credential, salt, member.Argon2.Iterations, member.Argon2.MemoryKiB, uint8(member.Argon2.Parallelism), 32)
	} else if member.Provider == "offline-keyfile" && keyfile {
		if len(credential) < 32 {
			return nil, errors.New("offline keyfile must contain at least 32 bytes")
		}
		info := fmt.Appendf(nil, "vaultic-capsule-keyfile\x00%d\x00%s", value.Header.Generation, member.MemberID)
		reader := hkdf.New(sha256.New, credential, []byte(value.Header.RepositoryID), info)
		key = make([]byte, 32)
		if _, err := reader.Read(key); err != nil {
			return nil, err
		}
	} else {
		return nil, errors.New("member credential type does not match provider")
	}
	defer clear(key)
	nonce, err := decodeBase64Fixed(member.Nonce, 12, "member nonce")
	if err != nil {
		return nil, err
	}
	wrapped, err := base64.StdEncoding.DecodeString(member.WrappedShare)
	if err != nil {
		return nil, fmt.Errorf("decode wrapped member share: %w", err)
	}
	aad, err := json.Marshal([]any{"vaultic-recovery-capsule-share", value.Header.RepositoryID, value.Header.Generation, value.Header.RootKeyVersion, value.Header.PolicyHash, member.GroupID, member.MemberID, member.ShareIndex, member.Threshold, member.ShareCount, member.Provider})
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	plaintext, err := aead.Open(nil, nonce, wrapped, aad)
	if err != nil {
		return nil, errors.New("member share authentication failed")
	}
	return plaintext, nil
}

func decodeBase64Fixed(encoded *string, size int, name string) ([]byte, error) {
	if encoded == nil {
		return nil, fmt.Errorf("missing %s", name)
	}
	decoded, err := base64.StdEncoding.DecodeString(*encoded)
	if err != nil || len(decoded) != size {
		return nil, fmt.Errorf("invalid %s", name)
	}
	return decoded, nil
}

func clear(value []byte) {
	for index := range value {
		value[index] = 0
	}
}
