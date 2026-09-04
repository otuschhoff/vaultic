package bootstrap

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/vaultic"
	"golang.org/x/crypto/hkdf"
)

const (
	Format           = 1
	MaxManifestBytes = 1 << 20
)

type Manifest struct {
	Format           uint                       `json:"format"`
	RepositoryID     string                     `json:"repository_id"`
	Generation       uint64                     `json:"generation"`
	CreatedAt        time.Time                  `json:"created_at"`
	ConfigSHA256     string                     `json:"config_sha256"`
	RepositoryConfig json.RawMessage            `json:"repository_config,omitempty"`
	Backends         []vaultic.PlacementBackend `json:"backends"`
	Policy           vaultic.PlacementPolicy    `json:"placement_policy"`
	StagingBackends  []string                   `json:"staging_backends"`
	PreviousSHA256   string                     `json:"previous_sha256,omitempty"`
}

type Anchor struct {
	RepositoryID string `json:"repository_id"`
	Generation   uint64 `json:"generation"`
	SHA256       string `json:"sha256"`
}

type Copy struct {
	Seed     string
	Manifest Manifest
	SHA256   string
}

type sealedManifest struct {
	Format       uint   `json:"format"`
	RepositoryID string `json:"repository_id"`
	Generation   uint64 `json:"generation"`
	Nonce        []byte `json:"nonce"`
	Ciphertext   []byte `json:"ciphertext"`
}

func (manifest Manifest) Validate() error {
	if manifest.Format != Format || manifest.RepositoryID == "" || manifest.Generation == 0 || manifest.CreatedAt.IsZero() {
		return fmt.Errorf("invalid bootstrap topology identity or version")
	}
	if !validDigest(manifest.ConfigSHA256) || manifest.PreviousSHA256 != "" && !validDigest(manifest.PreviousSHA256) {
		return fmt.Errorf("invalid bootstrap topology digest")
	}
	if len(manifest.RepositoryConfig) > 0 {
		cfg, err := manifest.ConfigProjection()
		if err != nil {
			return err
		}
		digest := sha256.Sum256(manifest.RepositoryConfig)
		if hex.EncodeToString(digest[:]) != manifest.ConfigSHA256 || cfg.ID != manifest.RepositoryID {
			return fmt.Errorf("bootstrap repository config projection identity or digest mismatch")
		}
		configuredBackends, _ := json.Marshal(cfg.PlacementBackends)
		manifestBackends, _ := json.Marshal(manifest.Backends)
		configuredPolicy, _ := json.Marshal(cfg.PlacementPolicy)
		manifestPolicy, _ := json.Marshal(manifest.Policy)
		configuredStaging, _ := json.Marshal(cfg.StagingBackends)
		manifestStaging, _ := json.Marshal(manifest.StagingBackends)
		if !bytes.Equal(configuredBackends, manifestBackends) || !bytes.Equal(configuredPolicy, manifestPolicy) ||
			!bytes.Equal(configuredStaging, manifestStaging) {
			return fmt.Errorf("bootstrap repository config projection disagrees with topology")
		}
	}
	if len(manifest.Backends) == 0 || len(manifest.StagingBackends) == 0 {
		return fmt.Errorf("bootstrap topology requires backends and staging mirrors")
	}
	seen := make(map[string]vaultic.PlacementBackend, len(manifest.Backends))
	for index, entry := range manifest.Backends {
		if entry.ID == "" || entry.Location == "" || entry.FailureDomain == "" || containsCredential(entry.Location) {
			return fmt.Errorf("backend %q has an incomplete or credential-bearing locator", entry.ID)
		}
		if _, exists := seen[entry.ID]; exists {
			return fmt.Errorf("duplicate backend id %q", entry.ID)
		}
		if index > 0 && manifest.Backends[index-1].ID >= entry.ID {
			return fmt.Errorf("bootstrap backends must be in canonical ID order")
		}
		seen[entry.ID] = entry
	}
	stagingSeen := make(map[string]struct{}, len(manifest.StagingBackends))
	for index, id := range manifest.StagingBackends {
		entry, ok := seen[id]
		if !ok || entry.Ingest != nil && !*entry.Ingest {
			return fmt.Errorf("staging backend %q is missing or not ingest-enabled", id)
		}
		if _, exists := stagingSeen[id]; exists {
			return fmt.Errorf("duplicate staging backend %q", id)
		}
		if index > 0 && manifest.StagingBackends[index-1] >= id {
			return fmt.Errorf("staging backends must be in canonical ID order")
		}
		stagingSeen[id] = struct{}{}
	}
	_, err := EvaluatePolicy(manifest.Backends, manifest.Policy)
	return err
}

func (manifest Manifest) ConfigProjection() (vaultic.Config, error) {
	if len(manifest.RepositoryConfig) == 0 {
		return vaultic.Config{}, fmt.Errorf("bootstrap topology has no repository config projection")
	}
	var cfg vaultic.Config
	decoder := json.NewDecoder(bytes.NewReader(manifest.RepositoryConfig))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cfg); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return vaultic.Config{}, fmt.Errorf("decode bootstrap repository config projection")
	}
	if err := cfg.Validate(); err != nil {
		return vaultic.Config{}, err
	}
	return cfg, nil
}

func EvaluatePolicy(backends []vaultic.PlacementBackend, policy vaultic.PlacementPolicy) ([]string, error) {
	if policy.MinCopies == 0 {
		policy.MinCopies = 1
	}
	if policy.MinDomains == 0 {
		policy.MinDomains = 1
	}
	var eligible []string
	domains := map[string]struct{}{}
	offsite := uint(0)
	for _, entry := range backends {
		if entry.Ingest != nil && !*entry.Ingest {
			continue
		}
		eligible = append(eligible, entry.ID)
		domains[entry.FailureDomain] = struct{}{}
		if entry.Offsite {
			offsite++
		}
	}
	if uint(len(eligible)) < policy.MinCopies || uint(len(domains)) < policy.MinDomains || offsite < policy.MinOffsite {
		return nil, fmt.Errorf(
			"reachable backends cannot satisfy placement policy copies=%d domains=%d offsite=%d",
			policy.MinCopies,
			policy.MinDomains,
			policy.MinOffsite,
		)
	}
	sort.Strings(eligible)
	return eligible, nil
}

func Seal(manifest Manifest, rootKey []byte) ([]byte, string, error) {
	if err := manifest.Validate(); err != nil {
		return nil, "", err
	}
	plaintext, err := json.Marshal(manifest)
	if err != nil {
		return nil, "", err
	}
	digest := sha256.Sum256(plaintext)
	key, err := deriveKey(rootKey, manifest.RepositoryID, "bootstrap-topology-v1")
	if err != nil {
		return nil, "", err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, "", err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, "", err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, "", err
	}
	aad := associatedData(manifest.RepositoryID, manifest.Generation)
	sealed := sealedManifest{
		Format:       Format,
		RepositoryID: manifest.RepositoryID,
		Generation:   manifest.Generation,
		Nonce:        nonce,
		Ciphertext:   aead.Seal(nil, nonce, plaintext, aad),
	}
	encoded, err := json.Marshal(sealed)
	return encoded, hex.EncodeToString(digest[:]), err
}

func Open(encoded, rootKey []byte, expectedRepository string) (Manifest, string, error) {
	return open(encoded, expectedRepository, func(repositoryID string) ([]byte, error) {
		return deriveKey(rootKey, repositoryID, "bootstrap-topology-v1")
	})
}

func OpenWithTopologyKey(encoded, topologyKey []byte, expectedRepository string) (Manifest, string, error) {
	return open(encoded, expectedRepository, func(string) ([]byte, error) {
		if len(topologyKey) != 32 {
			return nil, fmt.Errorf("invalid topology discovery key")
		}
		return topologyKey, nil
	})
}

func open(encoded []byte, expectedRepository string, keyFor func(string) ([]byte, error)) (Manifest, string, error) {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var sealed sealedManifest
	if err := decoder.Decode(&sealed); err != nil {
		return Manifest{}, "", fmt.Errorf("decode sealed bootstrap topology: %w", err)
	}
	if decoder.Decode(&struct{}{}) != io.EOF || sealed.Format != Format || sealed.RepositoryID != expectedRepository || sealed.Generation == 0 {
		return Manifest{}, "", fmt.Errorf("invalid sealed bootstrap topology identity")
	}
	key, err := keyFor(sealed.RepositoryID)
	if err != nil {
		return Manifest{}, "", err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return Manifest{}, "", err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return Manifest{}, "", err
	}
	plaintext, err := aead.Open(nil, sealed.Nonce, sealed.Ciphertext, associatedData(sealed.RepositoryID, sealed.Generation))
	if err != nil {
		return Manifest{}, "", fmt.Errorf("authenticate bootstrap topology: %w", err)
	}
	var manifest Manifest
	decoder = json.NewDecoder(bytes.NewReader(plaintext))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return Manifest{}, "", fmt.Errorf("decode bootstrap topology payload")
	}
	if manifest.RepositoryID != sealed.RepositoryID || manifest.Generation != sealed.Generation {
		return Manifest{}, "", fmt.Errorf("bootstrap topology envelope mismatch")
	}
	if err := manifest.Validate(); err != nil {
		return Manifest{}, "", err
	}
	digest := sha256.Sum256(plaintext)
	return manifest, hex.EncodeToString(digest[:]), nil
}

func Resolve(copies []Copy, trusted ...Anchor) (Copy, error) {
	if len(copies) == 0 {
		return Copy{}, fmt.Errorf("no authenticated bootstrap topology is reachable")
	}
	sort.Slice(copies, func(i, j int) bool { return copies[i].Manifest.Generation > copies[j].Manifest.Generation })
	winner := copies[0]
	for _, copy := range copies {
		if copy.Manifest.RepositoryID != winner.Manifest.RepositoryID {
			return Copy{}, fmt.Errorf("bootstrap topology repository conflict")
		}
		if copy.Manifest.Generation == winner.Manifest.Generation && copy.SHA256 != winner.SHA256 {
			return Copy{}, fmt.Errorf("bootstrap topology same-generation conflict")
		}
	}
	byGeneration := make(map[uint64]Copy, len(copies))
	for _, copy := range copies {
		byGeneration[copy.Manifest.Generation] = copy
	}
	for generation, copy := range byGeneration {
		if previous, ok := byGeneration[generation-1]; ok && copy.Manifest.PreviousSHA256 != previous.SHA256 {
			return Copy{}, fmt.Errorf("bootstrap topology generation chain conflict")
		}
	}
	for _, anchor := range trusted {
		if anchor.RepositoryID != winner.Manifest.RepositoryID || winner.Manifest.Generation < anchor.Generation ||
			winner.Manifest.Generation == anchor.Generation && winner.SHA256 != anchor.SHA256 {
			return Copy{}, fmt.Errorf("bootstrap topology violates trusted generation anchor")
		}
	}
	return winner, nil
}

func Handle(generation uint64) backend.Handle {
	return backend.Handle{Type: backend.BootstrapFile, Name: fmt.Sprintf("topology-%020d", generation)}
}

func Publish(ctx context.Context, mirrors map[string]backend.Backend, generation uint64, encoded []byte) error {
	if len(mirrors) == 0 || len(encoded) == 0 || len(encoded) > MaxManifestBytes {
		return fmt.Errorf("invalid bootstrap topology publication")
	}
	handle := Handle(generation)
	for id, mirror := range mirrors {
		if existing, err := load(ctx, mirror, handle); err == nil {
			if !bytes.Equal(existing, encoded) {
				return fmt.Errorf("bootstrap topology conflict on mirror %s", id)
			}
			continue
		} else if !mirror.IsNotExist(err) {
			return fmt.Errorf("inspect bootstrap mirror %s: %w", id, err)
		}
		if err := mirror.Save(ctx, handle, backend.NewByteReader(encoded, mirror.Hasher())); err != nil {
			existing, loadErr := load(ctx, mirror, handle)
			if loadErr != nil || !bytes.Equal(existing, encoded) {
				return fmt.Errorf("publish bootstrap topology to mirror %s: %w", id, err)
			}
		}
		stored, err := load(ctx, mirror, handle)
		if err != nil || !bytes.Equal(stored, encoded) {
			return fmt.Errorf("verify bootstrap topology on mirror %s", id)
		}
	}
	return nil
}

func Discover(ctx context.Context, seeds map[string]backend.Backend, rootKey []byte, repositoryID string) ([]Copy, map[string]error) {
	return discover(ctx, seeds, func(encoded []byte) (Manifest, string, error) {
		return Open(encoded, rootKey, repositoryID)
	})
}

func DiscoverWithTopologyKey(ctx context.Context, seeds map[string]backend.Backend, topologyKey []byte, repositoryID string) ([]Copy, map[string]error) {
	return discover(ctx, seeds, func(encoded []byte) (Manifest, string, error) {
		return OpenWithTopologyKey(encoded, topologyKey, repositoryID)
	})
}

func discover(
	ctx context.Context,
	seeds map[string]backend.Backend,
	opener func([]byte) (Manifest, string, error),
) ([]Copy, map[string]error) {
	copies := make([]Copy, 0)
	failures := make(map[string]error)
	for seedID, seed := range seeds {
		err := seed.List(ctx, backend.BootstrapFile, func(info backend.FileInfo) error {
			generation, ok := topologyGeneration(info.Name)
			if !ok {
				return nil
			}
			encoded, err := load(ctx, seed, Handle(generation))
			if err != nil {
				return err
			}
			manifest, digest, err := opener(encoded)
			if err != nil {
				return err
			}
			copies = append(copies, Copy{Seed: seedID, Manifest: manifest, SHA256: digest})
			return nil
		})
		if err != nil {
			failures[seedID] = err
		}
	}
	return copies, failures
}

func load(ctx context.Context, source backend.Backend, handle backend.Handle) ([]byte, error) {
	info, err := source.Stat(ctx, handle)
	if err != nil {
		return nil, err
	}
	if info.Size <= 0 || info.Size > MaxManifestBytes {
		return nil, fmt.Errorf("bootstrap topology object has invalid size")
	}
	var encoded []byte
	err = source.Load(ctx, handle, int(info.Size), 0, func(reader io.Reader) error {
		var err error
		encoded, err = io.ReadAll(io.LimitReader(reader, MaxManifestBytes+1))
		return err
	})
	return encoded, err
}

func topologyGeneration(name string) (uint64, bool) {
	value, ok := strings.CutPrefix(name, "topology-")
	if !ok {
		value, ok = strings.CutPrefix(name, "topology/")
	}
	if !ok || len(value) != 20 {
		return 0, false
	}
	generation, err := strconv.ParseUint(value, 10, 64)
	return generation, err == nil && generation > 0
}

func deriveKey(root []byte, repositoryID, purpose string) ([]byte, error) {
	if len(root) < 32 || repositoryID == "" {
		return nil, fmt.Errorf("bootstrap topology requires a repository key")
	}
	key := make([]byte, 32)
	_, err := io.ReadFull(hkdf.New(sha256.New, root, []byte(repositoryID), []byte("vaultic/"+purpose)), key)
	return key, err
}

func associatedData(repositoryID string, generation uint64) []byte {
	data := append([]byte("vaultic-bootstrap-topology-v1\x00"), repositoryID...)
	data = append(data, 0)
	return binary.BigEndian.AppendUint64(data, generation)
}

func validDigest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}

func containsCredential(location string) bool {
	beforeQuery := strings.SplitN(location, "?", 2)[0]
	if strings.Contains(beforeQuery, "@") {
		return true
	}
	queryIndex := strings.IndexByte(location, '?')
	if queryIndex < 0 {
		return false
	}
	query, err := url.ParseQuery(location[queryIndex+1:])
	if err != nil {
		return true
	}
	for key := range query {
		key = strings.ToLower(key)
		if strings.Contains(key, "token") || strings.Contains(key, "secret") || strings.Contains(key, "password") || strings.Contains(key, "access_key") ||
			strings.Contains(key, "credential") ||
			strings.Contains(key, "signature") {
			return true
		}
	}
	return false
}
