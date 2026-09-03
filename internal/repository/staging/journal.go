package staging

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
	"sort"
	"strings"
	"time"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/vaultic"
	"golang.org/x/crypto/hkdf"
)

const (
	Format          = 1
	MaxSegmentBytes = 8 << 20
)

type State string

const (
	StateUploading     State = "uploading"
	StateSealedPending State = "sealed-pending"
	StateCommitting    State = "committing"
	StateCommitted     State = "committed"
	StateAbandoned     State = "abandoned"
	StateExpired       State = "expired"
)

type Header struct {
	Format                  uint      `json:"format"`
	RepositoryID            string    `json:"repository_id"`
	JobID                   string    `json:"job_id"`
	IdempotencyKey          string    `json:"idempotency_key"`
	CreatedAt               time.Time `json:"created_at"`
	ExpiresAt               time.Time `json:"expires_at"`
	CapsuleGeneration       uint64    `json:"capsule_generation"`
	RepositoryKeyVersion    uint32    `json:"repository_key_version"`
	ChunkerVersion          string    `json:"chunker_version"`
	CompressionVersion      string    `json:"compression_version"`
	PlacementPolicyVersion  uint64    `json:"placement_policy_version"`
	SourceIdentitySHA256    string    `json:"source_identity_sha256"`
	ConsistencyEvidence     string    `json:"consistency_evidence"`
	BasisSnapshot           string    `json:"basis_snapshot,omitempty"`
	BasisMetadataGeneration uint64    `json:"basis_metadata_generation,omitempty"`
}

type Placement struct {
	BackendID     string `json:"backend_id"`
	FailureDomain string `json:"failure_domain"`
	Offsite       bool   `json:"offsite"`
	Size          int64  `json:"size"`
	SHA256        string `json:"sha256"`
}

type Pack struct {
	ID         string      `json:"id"`
	Type       string      `json:"type"`
	Size       int64       `json:"size"`
	SHA256     string      `json:"sha256"`
	Placements []Placement `json:"placements"`
}

type Record struct {
	Kind    string          `json:"kind"`
	Payload json.RawMessage `json:"payload"`
}

type Segment struct {
	Header         Header   `json:"header"`
	Sequence       uint64   `json:"sequence"`
	PreviousSHA256 string   `json:"previous_sha256,omitempty"`
	Packs          []Pack   `json:"packs,omitempty"`
	Records        []Record `json:"records,omitempty"`
}

type Seal struct {
	Header         Header    `json:"header"`
	State          State     `json:"state"`
	SegmentSHA256  []string  `json:"segment_sha256"`
	PackCount      uint64    `json:"pack_count"`
	ProtectedBytes uint64    `json:"protected_bytes"`
	SealedAt       time.Time `json:"sealed_at"`
}

type Completion struct {
	Header              Header    `json:"header"`
	State               State     `json:"state"`
	SealSHA256          string    `json:"seal_sha256"`
	MetadataTransaction string    `json:"metadata_transaction"`
	SnapshotID          string    `json:"snapshot_id"`
	CompletedAt         time.Time `json:"completed_at"`
}

type Abandonment struct {
	Header          Header    `json:"header"`
	State           State     `json:"state"`
	SealSHA256      string    `json:"seal_sha256"`
	Reason          string    `json:"reason"`
	Acknowledgement string    `json:"acknowledgement"`
	AbandonedAt     time.Time `json:"abandoned_at"`
	DeleteAfter     time.Time `json:"delete_after"`
}

type Job struct {
	Header         Header            `json:"header"`
	State          State             `json:"state"`
	Seal           Seal              `json:"seal"`
	SealSHA256     string            `json:"seal_sha256"`
	Completion     *Completion       `json:"completion,omitempty"`
	Abandonment    *Abandonment      `json:"abandonment,omitempty"`
	MirrorFailures map[string]string `json:"mirror_failures,omitempty"`
}

type Store struct {
	Mirrors                map[string]backend.Backend
	Key                    []byte
	Policy                 Policy
	Now                    func() time.Time
	AbandonmentSafetyDelay time.Duration
}

type PackRoots struct {
	Store        Store
	RepositoryID string
}

func (roots PackRoots) Current(ctx context.Context) (vaultic.IDSet, error) {
	jobs, err := roots.Store.Discover(ctx, roots.RepositoryID)
	if err != nil {
		return nil, err
	}
	protected := vaultic.NewIDSet()
	for _, job := range jobs {
		if job.State != StateSealedPending && job.State != StateExpired && !(job.State == StateAbandoned && job.Abandonment != nil && roots.Store.now().Before(job.Abandonment.DeleteAfter)) {
			continue
		}
		segments, err := roots.Store.VerifyJob(ctx, job)
		if err != nil {
			return nil, err
		}
		for _, segment := range segments {
			for _, pack := range segment.Packs {
				id, err := vaultic.ParseID(pack.ID)
				if err != nil {
					return nil, fmt.Errorf("invalid staged pack ID %q: %w", pack.ID, err)
				}
				protected.Insert(id)
			}
		}
	}
	return protected, nil
}

type Policy struct {
	MinCopies  uint
	MinDomains uint
	MinOffsite uint
}

type Quota struct {
	MaxBytes        uint64
	MaxJobs         uint64
	MaxAge          time.Duration
	MaxPerPrincipal uint64
}

func DeriveJournalKey(repositoryKey []byte, repositoryID string) ([]byte, error) {
	if len(repositoryKey) == 0 || repositoryID == "" {
		return nil, fmt.Errorf("repository key and identity are required for staging")
	}
	reader := hkdf.New(sha256.New, repositoryKey, []byte(repositoryID), []byte("vaultic-staging-journal-v1"))
	key := make([]byte, 32)
	if _, err := io.ReadFull(reader, key); err != nil {
		return nil, fmt.Errorf("derive staging journal key: %w", err)
	}
	return key, nil
}

type encryptedObject struct {
	Format       uint   `json:"format"`
	RepositoryID string `json:"repository_id"`
	JobID        string `json:"job_id"`
	Kind         string `json:"kind"`
	Sequence     uint64 `json:"sequence"`
	Nonce        []byte `json:"nonce"`
	Ciphertext   []byte `json:"ciphertext"`
}

func (header Header) Validate(now time.Time) error {
	if header.Format != Format || header.RepositoryID == "" || header.JobID == "" || header.IdempotencyKey == "" || header.CreatedAt.IsZero() || !header.ExpiresAt.After(header.CreatedAt) {
		return fmt.Errorf("invalid ingest journal identity or lifetime")
	}
	if header.ExpiresAt.Before(now) {
		return fmt.Errorf("ingest journal is expired")
	}
	if !validDigest(header.SourceIdentitySHA256) || header.CapsuleGeneration == 0 || header.RepositoryKeyVersion == 0 || header.PlacementPolicyVersion == 0 {
		return fmt.Errorf("incomplete ingest journal cryptographic context")
	}
	return nil
}

func (segment Segment) Validate(now time.Time, policy Policy) error {
	if err := segment.Header.Validate(now); err != nil {
		return err
	}
	if segment.Sequence == 0 || segment.Sequence > 1 && !validDigest(segment.PreviousSHA256) {
		return fmt.Errorf("invalid journal segment chain")
	}
	for _, pack := range segment.Packs {
		if err := validatePack(pack, policy); err != nil {
			return err
		}
	}
	encoded, err := json.Marshal(segment)
	if err != nil {
		return err
	}
	if len(encoded) > MaxSegmentBytes {
		return fmt.Errorf("journal segment exceeds %d bytes", MaxSegmentBytes)
	}
	return nil
}

func SealSegments(header Header, segments [][]byte, key []byte, policy Policy, now time.Time) ([]byte, Seal, string, error) {
	if err := header.Validate(now); err != nil {
		return nil, Seal{}, "", err
	}
	seal := Seal{Header: header, State: StateSealedPending, SealedAt: now}
	previous := ""
	for index, encoded := range segments {
		var segment Segment
		if _, err := openObject(encoded, key, header, "segment", uint64(index+1), &segment); err != nil {
			return nil, Seal{}, "", err
		}
		if segment.Sequence != uint64(index+1) || segment.PreviousSHA256 != previous {
			return nil, Seal{}, "", fmt.Errorf("journal segment chain is discontinuous")
		}
		if err := segment.Validate(now, policy); err != nil {
			return nil, Seal{}, "", err
		}
		digest := sha256.Sum256(encoded)
		previous = hex.EncodeToString(digest[:])
		seal.SegmentSHA256 = append(seal.SegmentSHA256, previous)
		seal.PackCount += uint64(len(segment.Packs))
		for _, pack := range segment.Packs {
			seal.ProtectedBytes += uint64(pack.Size)
		}
	}
	if len(seal.SegmentSHA256) == 0 {
		return nil, Seal{}, "", fmt.Errorf("cannot seal an empty journal")
	}
	encoded, digest, err := sealObject(seal, key, header, "seal", 0)
	return encoded, seal, digest, err
}

func SealSegment(segment Segment, key []byte, policy Policy, now time.Time) ([]byte, string, error) {
	if err := segment.Validate(now, policy); err != nil {
		return nil, "", err
	}
	return sealObject(segment, key, segment.Header, "segment", segment.Sequence)
}

func OpenSeal(encoded, key []byte, expected Header) (Seal, string, error) {
	var seal Seal
	digest, err := openObject(encoded, key, expected, "seal", 0, &seal)
	if err != nil {
		return Seal{}, "", err
	}
	if seal.State != StateSealedPending || len(seal.SegmentSHA256) == 0 {
		return Seal{}, "", fmt.Errorf("invalid journal seal state")
	}
	return seal, digest, nil
}

func SealCompletion(completion Completion, key []byte) ([]byte, string, error) {
	if completion.State != StateCommitted || !validDigest(completion.SealSHA256) || completion.MetadataTransaction == "" || completion.SnapshotID == "" || completion.CompletedAt.IsZero() {
		return nil, "", fmt.Errorf("invalid journal completion")
	}
	return sealObject(completion, key, completion.Header, "completion", 0)
}

func SealAbandonment(abandonment Abandonment, key []byte) ([]byte, string, error) {
	if abandonment.State != StateAbandoned || !validDigest(abandonment.SealSHA256) || abandonment.Reason == "" || abandonment.Acknowledgement == "" || abandonment.AbandonedAt.IsZero() || !abandonment.DeleteAfter.After(abandonment.AbandonedAt) {
		return nil, "", fmt.Errorf("invalid journal abandonment")
	}
	return sealObject(abandonment, key, abandonment.Header, "abandonment", 0)
}

func (store Store) PublishAbandonment(ctx context.Context, job Job, reason, acknowledgement string) (Abandonment, error) {
	if job.State != StateSealedPending && job.State != StateExpired {
		return Abandonment{}, fmt.Errorf("only an uncommitted sealed journal can be abandoned")
	}
	delay := store.AbandonmentSafetyDelay
	if delay <= 0 {
		delay = 24 * time.Hour
	}
	now := store.now()
	abandonment := Abandonment{Header: job.Header, State: StateAbandoned, SealSHA256: job.SealSHA256, Reason: reason, Acknowledgement: acknowledgement, AbandonedAt: now, DeleteAfter: now.Add(delay)}
	encoded, _, err := SealAbandonment(abandonment, store.Key)
	if err != nil {
		return Abandonment{}, err
	}
	if err := Publish(ctx, store.Mirrors, AbandonmentHandle(job.Header.JobID), encoded); err != nil {
		return Abandonment{}, err
	}
	return abandonment, nil
}

func Publish(ctx context.Context, mirrors map[string]backend.Backend, handle backend.Handle, encoded []byte) error {
	if handle.Type != backend.StagingFile || len(mirrors) == 0 || len(encoded) == 0 || len(encoded) > MaxSegmentBytes {
		return fmt.Errorf("journal publication requires staging mirrors")
	}
	for id, mirror := range mirrors {
		if existing, err := loadObject(ctx, mirror, handle); err == nil {
			if !bytes.Equal(existing, encoded) {
				return fmt.Errorf("journal object conflicts with immutable object on %s", id)
			}
			continue
		} else if !mirror.IsNotExist(err) {
			return fmt.Errorf("inspect journal object on %s: %w", id, err)
		}
		if err := mirror.Save(ctx, handle, backend.NewByteReader(encoded, mirror.Hasher())); err != nil {
			existing, loadErr := loadObject(ctx, mirror, handle)
			if loadErr != nil || !bytes.Equal(existing, encoded) {
				return fmt.Errorf("publish journal object to %s: %w", id, err)
			}
		}
		stored, err := loadObject(ctx, mirror, handle)
		if err != nil || !bytes.Equal(stored, encoded) {
			return fmt.Errorf("verify journal object on %s", id)
		}
	}
	return nil
}

func (store Store) PublishSegment(ctx context.Context, segment Segment) (string, error) {
	now := store.now()
	encoded, digest, err := SealSegment(segment, store.Key, store.Policy, now)
	if err != nil {
		return "", err
	}
	if err := Publish(ctx, store.Mirrors, SegmentHandle(segment.Header.JobID, segment.Sequence), encoded); err != nil {
		return "", err
	}
	return digest, nil
}

func (store Store) PublishSeal(ctx context.Context, header Header, segmentCount uint64) (Seal, string, error) {
	if segmentCount == 0 {
		return Seal{}, "", fmt.Errorf("cannot seal an empty journal")
	}
	segments := make([][]byte, segmentCount)
	for sequence := uint64(1); sequence <= segmentCount; sequence++ {
		handle := SegmentHandle(header.JobID, sequence)
		var authoritative []byte
		for id, mirror := range store.Mirrors {
			encoded, err := loadObject(ctx, mirror, handle)
			if err != nil {
				return Seal{}, "", fmt.Errorf("read journal segment %d from %s: %w", sequence, id, err)
			}
			if authoritative == nil {
				authoritative = encoded
			} else if !bytes.Equal(authoritative, encoded) {
				return Seal{}, "", fmt.Errorf("journal segment %d conflicts across mirrors", sequence)
			}
		}
		segments[sequence-1] = authoritative
	}
	encoded, seal, digest, err := SealSegments(header, segments, store.Key, store.Policy, store.now())
	if err != nil {
		return Seal{}, "", err
	}
	if err := Publish(ctx, store.Mirrors, SealHandle(header.JobID), encoded); err != nil {
		return Seal{}, "", err
	}
	return seal, digest, nil
}

func (store Store) Discover(ctx context.Context, repositoryID string) ([]Job, error) {
	type sealCopy struct {
		mirror string
		bytes  []byte
	}
	copies := map[string][]sealCopy{}
	failures := map[string]string{}
	for id, mirror := range store.Mirrors {
		err := mirror.List(ctx, backend.StagingFile, func(info backend.FileInfo) error {
			jobID, ok := strings.CutSuffix(info.Name, "/seal")
			if !ok || jobID == "" || strings.Contains(jobID, "/") {
				return nil
			}
			encoded, err := loadObject(ctx, mirror, SealHandle(jobID))
			if err != nil {
				return err
			}
			copies[jobID] = append(copies[jobID], sealCopy{mirror: id, bytes: encoded})
			return nil
		})
		if err != nil {
			failures[id] = err.Error()
		}
	}
	jobs := make([]Job, 0, len(copies))
	for jobID, candidates := range copies {
		first := candidates[0].bytes
		for _, candidate := range candidates[1:] {
			if !bytes.Equal(first, candidate.bytes) {
				return nil, fmt.Errorf("conflicting seal for journal %s", jobID)
			}
		}
		header := Header{RepositoryID: repositoryID, JobID: jobID}
		var object encryptedObject
		if err := json.Unmarshal(first, &object); err != nil {
			return nil, fmt.Errorf("decode journal %s envelope: %w", jobID, err)
		}
		header.Format = object.Format
		var seal Seal
		if _, err := openObject(first, store.Key, header, "seal", 0, &seal); err != nil {
			return nil, err
		}
		expired := seal.Header.ExpiresAt.Before(store.now())
		if err := seal.Header.Validate(seal.Header.CreatedAt); err != nil {
			return nil, err
		}
		if seal.Header.RepositoryID != repositoryID || seal.Header.JobID != jobID || seal.State != StateSealedPending {
			return nil, fmt.Errorf("journal seal identity mismatch")
		}
		digest := sha256.Sum256(first)
		state := StateSealedPending
		if expired {
			state = StateExpired
		}
		job := Job{Header: seal.Header, State: state, Seal: seal, SealSHA256: hex.EncodeToString(digest[:]), MirrorFailures: failures}
		for _, candidate := range candidates {
			completionBytes, err := loadObject(ctx, store.Mirrors[candidate.mirror], CompletionHandle(jobID))
			if err != nil {
				if !store.Mirrors[candidate.mirror].IsNotExist(err) {
					job.MirrorFailures[candidate.mirror] = err.Error()
				}
				continue
			}
			var completion Completion
			if _, err := openObject(completionBytes, store.Key, seal.Header, "completion", 0, &completion); err != nil {
				return nil, err
			}
			if completion.State != StateCommitted || completion.SealSHA256 != job.SealSHA256 {
				return nil, fmt.Errorf("journal completion does not bind the discovered seal")
			}
			job.State, job.Completion = StateCommitted, &completion
			break
		}
		if job.State != StateCommitted {
			for _, candidate := range candidates {
				abandonmentBytes, err := loadObject(ctx, store.Mirrors[candidate.mirror], AbandonmentHandle(jobID))
				if err != nil {
					if !store.Mirrors[candidate.mirror].IsNotExist(err) {
						job.MirrorFailures[candidate.mirror] = err.Error()
					}
					continue
				}
				var abandonment Abandonment
				if _, err := openObject(abandonmentBytes, store.Key, seal.Header, "abandonment", 0, &abandonment); err != nil {
					return nil, err
				}
				if abandonment.State != StateAbandoned || abandonment.SealSHA256 != job.SealSHA256 {
					return nil, fmt.Errorf("journal abandonment does not bind the discovered seal")
				}
				job.State, job.Abandonment = StateAbandoned, &abandonment
				break
			}
		}
		jobs = append(jobs, job)
	}
	sort.Slice(jobs, func(i, j int) bool { return jobs[i].Header.CreatedAt.Before(jobs[j].Header.CreatedAt) })
	return jobs, nil
}

func (store Store) VerifyJob(ctx context.Context, job Job) ([]Segment, error) {
	if job.State != StateSealedPending && job.State != StateCommitted && job.State != StateExpired && job.State != StateAbandoned {
		return nil, fmt.Errorf("journal %s is not a sealed reachability root", job.Header.JobID)
	}
	if len(job.Seal.SegmentSHA256) == 0 || job.Seal.Header != job.Header {
		return nil, fmt.Errorf("journal seal header mismatch")
	}
	segments := make([]Segment, len(job.Seal.SegmentSHA256))
	previous := ""
	for index, expectedDigest := range job.Seal.SegmentSHA256 {
		sequence := uint64(index + 1)
		var authoritative []byte
		for id, mirror := range store.Mirrors {
			encoded, err := loadObject(ctx, mirror, SegmentHandle(job.Header.JobID, sequence))
			if err != nil {
				return nil, fmt.Errorf("load sealed journal segment %d from %s: %w", sequence, id, err)
			}
			if authoritative == nil {
				authoritative = encoded
			} else if !bytes.Equal(authoritative, encoded) {
				return nil, fmt.Errorf("sealed journal segment %d conflicts across mirrors", sequence)
			}
		}
		digest := sha256.Sum256(authoritative)
		if hex.EncodeToString(digest[:]) != expectedDigest {
			return nil, fmt.Errorf("sealed journal segment %d digest mismatch", sequence)
		}
		if _, err := openObject(authoritative, store.Key, job.Header, "segment", sequence, &segments[index]); err != nil {
			return nil, err
		}
		if segments[index].Sequence != sequence || segments[index].PreviousSHA256 != previous {
			return nil, fmt.Errorf("sealed journal segment chain is discontinuous")
		}
		validationTime := store.now()
		if !job.Header.ExpiresAt.After(validationTime) {
			validationTime = job.Header.ExpiresAt.Add(-time.Nanosecond)
		}
		if err := segments[index].Validate(validationTime, store.Policy); err != nil {
			return nil, err
		}
		previous = expectedDigest
	}
	return segments, nil
}

func (store Store) now() time.Time {
	if store.Now != nil {
		return store.Now().UTC()
	}
	return time.Now().UTC()
}

func loadObject(ctx context.Context, source backend.Backend, handle backend.Handle) ([]byte, error) {
	info, err := source.Stat(ctx, handle)
	if err != nil {
		return nil, err
	}
	if info.Size <= 0 || info.Size > MaxSegmentBytes*2 {
		return nil, fmt.Errorf("journal object has invalid size")
	}
	var encoded []byte
	err = source.Load(ctx, handle, int(info.Size), 0, func(reader io.Reader) error {
		var err error
		encoded, err = io.ReadAll(io.LimitReader(reader, MaxSegmentBytes*2+1))
		return err
	})
	return encoded, err
}

func SegmentHandle(jobID string, sequence uint64) backend.Handle {
	return backend.Handle{Type: backend.StagingFile, Name: fmt.Sprintf("%s/segments/%020d", jobID, sequence)}
}

func SealHandle(jobID string) backend.Handle {
	return backend.Handle{Type: backend.StagingFile, Name: jobID + "/seal"}
}

func CompletionHandle(jobID string) backend.Handle {
	return backend.Handle{Type: backend.StagingFile, Name: jobID + "/completion"}
}

func AbandonmentHandle(jobID string) backend.Handle {
	return backend.Handle{Type: backend.StagingFile, Name: jobID + "/abandonment"}
}

func CheckQuota(quota Quota, activeJobs, principalJobs, stagedBytes uint64, oldest time.Time, additional uint64, now time.Time) error {
	if quota.MaxJobs > 0 && activeJobs >= quota.MaxJobs || quota.MaxPerPrincipal > 0 && principalJobs >= quota.MaxPerPrincipal || quota.MaxBytes > 0 && stagedBytes+additional > quota.MaxBytes || quota.MaxAge > 0 && !oldest.IsZero() && now.Sub(oldest) > quota.MaxAge {
		return fmt.Errorf("deferred staging quota exceeded")
	}
	return nil
}

func validatePack(pack Pack, policy Policy) error {
	if pack.ID == "" || pack.Size <= 0 || !validDigest(pack.SHA256) {
		return fmt.Errorf("invalid staged pack")
	}
	seen := map[string]struct{}{}
	domains := map[string]struct{}{}
	offsite := uint(0)
	for _, placement := range pack.Placements {
		if placement.BackendID == "" || placement.FailureDomain == "" || placement.Size != pack.Size || placement.SHA256 != pack.SHA256 {
			return fmt.Errorf("pack %s has an invalid placement proof", pack.ID)
		}
		if _, ok := seen[placement.BackendID]; ok {
			return fmt.Errorf("pack %s repeats backend %s", pack.ID, placement.BackendID)
		}
		seen[placement.BackendID] = struct{}{}
		domains[placement.FailureDomain] = struct{}{}
		if placement.Offsite {
			offsite++
		}
	}
	if uint(len(seen)) < max(1, policy.MinCopies) || uint(len(domains)) < max(1, policy.MinDomains) || offsite < policy.MinOffsite {
		return fmt.Errorf("pack %s does not satisfy staging durability", pack.ID)
	}
	return nil
}

func sealObject(value any, rootKey []byte, header Header, kind string, sequence uint64) ([]byte, string, error) {
	plaintext, err := json.Marshal(value)
	if err != nil {
		return nil, "", err
	}
	aead, err := journalAEAD(rootKey, header)
	if err != nil {
		return nil, "", err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, "", err
	}
	object := encryptedObject{Format: Format, RepositoryID: header.RepositoryID, JobID: header.JobID, Kind: kind, Sequence: sequence, Nonce: nonce}
	object.Ciphertext = aead.Seal(nil, nonce, plaintext, objectAAD(object))
	encoded, err := json.Marshal(object)
	digest := sha256.Sum256(encoded)
	return encoded, hex.EncodeToString(digest[:]), err
}

func openObject(encoded, rootKey []byte, header Header, kind string, sequence uint64, target any) (string, error) {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var object encryptedObject
	if err := decoder.Decode(&object); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return "", fmt.Errorf("decode encrypted journal object")
	}
	if object.Format != Format || object.RepositoryID != header.RepositoryID || object.JobID != header.JobID || object.Kind != kind || object.Sequence != sequence {
		return "", fmt.Errorf("journal object context mismatch")
	}
	aead, err := journalAEAD(rootKey, header)
	if err != nil {
		return "", err
	}
	plaintext, err := aead.Open(nil, object.Nonce, object.Ciphertext, objectAAD(object))
	if err != nil {
		return "", fmt.Errorf("authenticate journal object: %w", err)
	}
	decoder = json.NewDecoder(bytes.NewReader(plaintext))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return "", fmt.Errorf("decode journal payload")
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func journalAEAD(rootKey []byte, header Header) (cipher.AEAD, error) {
	if len(rootKey) < 32 {
		return nil, fmt.Errorf("journal key material is too short")
	}
	key := make([]byte, 32)
	info := []byte("vaultic/staging-journal-v1/" + header.JobID)
	if _, err := io.ReadFull(hkdf.New(sha256.New, rootKey, []byte(header.RepositoryID), info), key); err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

func objectAAD(object encryptedObject) []byte {
	parts := []string{"vaultic-staging-v1", object.RepositoryID, object.JobID, object.Kind}
	data := []byte(strings.Join(parts, "\x00"))
	return binary.BigEndian.AppendUint64(data, object.Sequence)
}

func validDigest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}

func SortPlacements(placements []Placement) {
	sort.Slice(placements, func(i, j int) bool { return placements[i].BackendID < placements[j].BackendID })
}
