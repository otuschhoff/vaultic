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
	StateRejected      State = "rejected"
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
	ID          string      `json:"id"`
	Type        string      `json:"type"`
	Size        int64       `json:"size"`
	PayloadSize uint64      `json:"payload_size"`
	HeaderSize  uint64      `json:"header_size"`
	BlobCount   uint64      `json:"blob_count"`
	SHA256      string      `json:"sha256"`
	Placements  []Placement `json:"placements"`
}

type BlobFact struct {
	ID                 string `json:"id"`
	Type               string `json:"type"`
	PackID             string `json:"pack_id"`
	Offset             uint   `json:"offset"`
	Length             uint   `json:"length"`
	UncompressedLength uint   `json:"uncompressed_length,omitempty"`
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

type Rejection struct {
	Header     Header    `json:"header"`
	State      State     `json:"state"`
	SealSHA256 string    `json:"seal_sha256"`
	Reason     string    `json:"reason"`
	RejectedAt time.Time `json:"rejected_at"`
}

type Extension struct {
	Header                  Header    `json:"header"`
	SealSHA256              string    `json:"seal_sha256"`
	Generation              uint64    `json:"generation"`
	PreviousExtensionSHA256 string    `json:"previous_extension_sha256,omitempty"`
	PreviousExpiresAt       time.Time `json:"previous_expires_at"`
	ExpiresAt               time.Time `json:"expires_at"`
	ExtendedAt              time.Time `json:"extended_at"`
}

type Job struct {
	Header          Header            `json:"header"`
	State           State             `json:"state"`
	Seal            Seal              `json:"seal"`
	SealSHA256      string            `json:"seal_sha256"`
	Completion      *Completion       `json:"completion,omitempty"`
	Abandonment     *Abandonment      `json:"abandonment,omitempty"`
	Rejection       *Rejection        `json:"rejection,omitempty"`
	Extension       *Extension        `json:"extension,omitempty"`
	ExtensionSHA256 string            `json:"extension_sha256,omitempty"`
	MirrorFailures  map[string]string `json:"mirror_failures,omitempty"`
}

func (job Job) EffectiveExpiresAt() time.Time {
	if job.Extension != nil {
		return job.Extension.ExpiresAt
	}
	return job.Header.ExpiresAt
}

type Store struct {
	Mirrors                map[string]backend.Backend
	MirrorPlacements       map[string]MirrorPlacement
	Key                    []byte
	Policy                 Policy
	Now                    func() time.Time
	AbandonmentSafetyDelay time.Duration
	MaxExtension           time.Duration
}

type MirrorPlacement struct {
	FailureDomain string
	Offsite       bool
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
		if job.State != StateSealedPending && job.State != StateExpired && job.State != StateRejected &&
			(job.State != StateAbandoned || job.Abandonment == nil || !roots.Store.now().Before(job.Abandonment.DeleteAfter)) {
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

type Usage struct {
	Jobs        uint64
	Bytes       uint64
	OldestJobAt time.Time
}

func (store Store) ActiveUsage(ctx context.Context, repositoryID string) (Usage, error) {
	jobs, err := store.Discover(ctx, repositoryID)
	if err != nil {
		return Usage{}, err
	}
	var usage Usage
	for _, job := range jobs {
		if job.State == StateCommitted || job.State == StateAbandoned && job.Abandonment != nil && !store.now().Before(job.Abandonment.DeleteAfter) {
			continue
		}
		usage.Jobs++
		usage.Bytes += job.Seal.ProtectedBytes
		if usage.OldestJobAt.IsZero() || job.Header.CreatedAt.Before(usage.OldestJobAt) {
			usage.OldestJobAt = job.Header.CreatedAt
		}
	}
	return usage, nil
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
	if header.Format != Format || header.RepositoryID == "" || header.JobID == "" || header.IdempotencyKey == "" || header.CreatedAt.IsZero() ||
		!header.ExpiresAt.After(header.CreatedAt) {
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
	if completion.State != StateCommitted || !validDigest(completion.SealSHA256) || completion.MetadataTransaction == "" || completion.SnapshotID == "" ||
		completion.CompletedAt.IsZero() {
		return nil, "", fmt.Errorf("invalid journal completion")
	}
	return sealObject(completion, key, completion.Header, "completion", 0)
}

func SealAbandonment(abandonment Abandonment, key []byte) ([]byte, string, error) {
	if abandonment.State != StateAbandoned || !validDigest(abandonment.SealSHA256) || abandonment.Reason == "" || abandonment.Acknowledgement == "" ||
		abandonment.AbandonedAt.IsZero() ||
		!abandonment.DeleteAfter.After(abandonment.AbandonedAt) {
		return nil, "", fmt.Errorf("invalid journal abandonment")
	}
	return sealObject(abandonment, key, abandonment.Header, "abandonment", 0)
}

func SealRejection(rejection Rejection, key []byte) ([]byte, string, error) {
	if rejection.State != StateRejected || !validDigest(rejection.SealSHA256) || rejection.Reason == "" || rejection.RejectedAt.IsZero() {
		return nil, "", fmt.Errorf("invalid journal rejection")
	}
	return sealObject(rejection, key, rejection.Header, "rejection", 0)
}

func SealExtension(extension Extension, key []byte) ([]byte, string, error) {
	if !validDigest(extension.SealSHA256) || extension.Generation == 0 || extension.Generation > 1 && !validDigest(extension.PreviousExtensionSHA256) ||
		extension.ExtendedAt.IsZero() ||
		!extension.ExpiresAt.After(extension.PreviousExpiresAt) ||
		extension.ExtendedAt.After(extension.ExpiresAt) {
		return nil, "", fmt.Errorf("invalid journal expiry extension")
	}
	return sealObject(extension, key, extension.Header, "extension", extension.Generation)
}

func (store Store) PublishExtension(ctx context.Context, job Job, expiresAt time.Time) (Extension, error) {
	if job.State == StateCommitted || job.State == StateAbandoned || job.State == StateRejected {
		return Extension{}, fmt.Errorf("terminal journal cannot be extended")
	}
	now := store.now()
	if store.MaxExtension > 0 && expiresAt.Sub(now) > store.MaxExtension {
		return Extension{}, fmt.Errorf("journal extension exceeds repository policy")
	}
	generation := uint64(1)
	if job.Extension != nil {
		generation = job.Extension.Generation + 1
	}
	extension := Extension{
		Header:                  job.Header,
		SealSHA256:              job.SealSHA256,
		Generation:              generation,
		PreviousExtensionSHA256: job.ExtensionSHA256,
		PreviousExpiresAt:       job.EffectiveExpiresAt(),
		ExpiresAt:               expiresAt.UTC(),
		ExtendedAt:              now,
	}
	encoded, _, err := SealExtension(extension, store.Key)
	if err != nil {
		return Extension{}, err
	}
	if err := store.publish(ctx, ExtensionHandle(job.Header.JobID, generation), encoded); err != nil {
		return Extension{}, err
	}
	return extension, nil
}

func (store Store) PublishRejection(ctx context.Context, job Job, reason string) (Rejection, error) {
	if job.State != StateSealedPending && job.State != StateExpired {
		return Rejection{}, fmt.Errorf("only an uncommitted sealed journal can be rejected")
	}
	rejection := Rejection{Header: job.Header, State: StateRejected, SealSHA256: job.SealSHA256, Reason: reason, RejectedAt: store.now()}
	encoded, _, err := SealRejection(rejection, store.Key)
	if err != nil {
		return Rejection{}, err
	}
	if err := store.publish(ctx, RejectionHandle(job.Header.JobID), encoded); err != nil {
		return Rejection{}, err
	}
	return rejection, nil
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
	abandonment := Abandonment{
		Header:          job.Header,
		State:           StateAbandoned,
		SealSHA256:      job.SealSHA256,
		Reason:          reason,
		Acknowledgement: acknowledgement,
		AbandonedAt:     now,
		DeleteAfter:     now.Add(delay),
	}
	encoded, _, err := SealAbandonment(abandonment, store.Key)
	if err != nil {
		return Abandonment{}, err
	}
	if err := store.publish(ctx, AbandonmentHandle(job.Header.JobID), encoded); err != nil {
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
	if err := store.publish(ctx, SegmentHandle(segment.Header.JobID, segment.Sequence), encoded); err != nil {
		return "", err
	}
	return digest, nil
}

func (store Store) PublishJob(ctx context.Context, header Header, packs []Pack, records []Record) (Seal, string, uint64, error) {
	segments := make([]Segment, 0, 1)
	current := Segment{Header: header, Sequence: 1}
	appendIfFits := func(pack *Pack, record *Record) bool {
		candidate := current
		candidate.Packs = append([]Pack(nil), current.Packs...)
		candidate.Records = append([]Record(nil), current.Records...)
		if pack != nil {
			candidate.Packs = append(candidate.Packs, *pack)
		}
		if record != nil {
			candidate.Records = append(candidate.Records, *record)
		}
		encoded, err := json.Marshal(candidate)
		if err != nil || len(encoded) > MaxSegmentBytes {
			return false
		}
		current = candidate
		return true
	}
	flush := func() error {
		if len(current.Packs) == 0 && len(current.Records) == 0 {
			return fmt.Errorf("journal item exceeds segment size limit")
		}
		segments = append(segments, current)
		current = Segment{Header: header, Sequence: uint64(len(segments) + 1)}
		return nil
	}
	for index := range packs {
		if !appendIfFits(&packs[index], nil) {
			if err := flush(); err != nil || !appendIfFits(&packs[index], nil) {
				return Seal{}, "", 0, fmt.Errorf("staged pack fact exceeds segment size limit")
			}
		}
	}
	for index := range records {
		if !appendIfFits(nil, &records[index]) {
			if err := flush(); err != nil || !appendIfFits(nil, &records[index]) {
				return Seal{}, "", 0, fmt.Errorf("staged journal record exceeds segment size limit")
			}
		}
	}
	if err := flush(); err != nil {
		return Seal{}, "", 0, err
	}
	previous := ""
	for index := range segments {
		segments[index].PreviousSHA256 = previous
		digest, err := store.PublishSegment(ctx, segments[index])
		if err != nil {
			return Seal{}, "", 0, err
		}
		previous = digest
	}
	seal, digest, err := store.PublishSeal(ctx, header, uint64(len(segments)))
	return seal, digest, uint64(len(segments)), err
}

func (store Store) PublishSeal(ctx context.Context, header Header, segmentCount uint64) (Seal, string, error) {
	if segmentCount == 0 {
		return Seal{}, "", fmt.Errorf("cannot seal an empty journal")
	}
	segments := make([][]byte, segmentCount)
	for sequence := uint64(1); sequence <= segmentCount; sequence++ {
		handle := SegmentHandle(header.JobID, sequence)
		authoritative, err := store.loadQuorum(ctx, handle)
		if err != nil {
			return Seal{}, "", fmt.Errorf("read journal segment %d: %w", sequence, err)
		}
		segments[sequence-1] = authoritative
	}
	encoded, seal, digest, err := SealSegments(header, segments, store.Key, store.Policy, store.now())
	if err != nil {
		return Seal{}, "", err
	}
	if err := store.publish(ctx, SealHandle(header.JobID), encoded); err != nil {
		return Seal{}, "", err
	}
	return seal, digest, nil
}

func (store Store) publish(ctx context.Context, handle backend.Handle, encoded []byte) error {
	successes := uint(0)
	offsite := uint(0)
	domains := make(map[string]struct{})
	failures := make(map[string]error)
	for id, mirror := range store.Mirrors {
		if err := Publish(ctx, map[string]backend.Backend{id: mirror}, handle, encoded); err != nil {
			failures[id] = err
			continue
		}
		successes++
		placement, ok := store.MirrorPlacements[id]
		if !ok {
			placement.FailureDomain = id
		}
		domains[placement.FailureDomain] = struct{}{}
		if placement.Offsite {
			offsite++
		}
	}
	if successes < store.Policy.MinCopies || uint(len(domains)) < store.Policy.MinDomains || offsite < store.Policy.MinOffsite {
		return fmt.Errorf("journal publication policy unsatisfied: %v", failures)
	}
	return nil
}

func (store Store) loadQuorum(ctx context.Context, handle backend.Handle) ([]byte, error) {
	var authoritative []byte
	successes := uint(0)
	offsite := uint(0)
	domains := make(map[string]struct{})
	failures := make(map[string]error)
	for id, mirror := range store.Mirrors {
		encoded, err := loadObject(ctx, mirror, handle)
		if err != nil {
			failures[id] = err
			continue
		}
		if authoritative == nil {
			authoritative = encoded
		} else if !bytes.Equal(authoritative, encoded) {
			return nil, fmt.Errorf("journal object conflicts across mirrors")
		}
		successes++
		placement, ok := store.MirrorPlacements[id]
		if !ok {
			placement.FailureDomain = id
		}
		domains[placement.FailureDomain] = struct{}{}
		if placement.Offsite {
			offsite++
		}
	}
	if successes < store.Policy.MinCopies || uint(len(domains)) < store.Policy.MinDomains || offsite < store.Policy.MinOffsite {
		return nil, fmt.Errorf("journal read policy unsatisfied: %v", failures)
	}
	return authoritative, nil
}

func (store Store) loadOptionalQuorum(ctx context.Context, handle backend.Handle) ([]byte, bool, error) {
	for _, mirror := range store.Mirrors {
		if _, err := mirror.Stat(ctx, handle); err == nil {
			encoded, err := store.loadQuorum(ctx, handle)
			return encoded, err == nil, err
		} else if !mirror.IsNotExist(err) {
			continue
		}
	}
	return nil, false, nil
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
			jobID, ok := strings.CutSuffix(info.Name, "--seal")
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
		first, err := store.loadQuorum(ctx, SealHandle(jobID))
		if err != nil {
			return nil, fmt.Errorf("load journal %s seal: %w", jobID, err)
		}
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
		if err := seal.Header.Validate(seal.Header.CreatedAt); err != nil {
			return nil, err
		}
		if seal.Header.RepositoryID != repositoryID || seal.Header.JobID != jobID || seal.State != StateSealedPending {
			return nil, fmt.Errorf("journal seal identity mismatch")
		}
		digest := sha256.Sum256(first)
		job := Job{Header: seal.Header, State: StateSealedPending, Seal: seal, SealSHA256: hex.EncodeToString(digest[:]), MirrorFailures: failures}
		previousExpiry := job.Header.ExpiresAt
		previousDigest := ""
		for generation := uint64(1); ; generation++ {
			extensionBytes, found, err := store.loadOptionalQuorum(ctx, ExtensionHandle(jobID, generation))
			if err != nil {
				return nil, fmt.Errorf("load journal %s extension %d: %w", jobID, generation, err)
			}
			if !found {
				break
			}
			var extension Extension
			digest, err := openObject(extensionBytes, store.Key, seal.Header, "extension", generation, &extension)
			if err != nil {
				return nil, err
			}
			if extension.SealSHA256 != job.SealSHA256 || extension.Generation != generation || extension.PreviousExpiresAt != previousExpiry ||
				extension.PreviousExtensionSHA256 != previousDigest ||
				!extension.ExpiresAt.After(extension.PreviousExpiresAt) {
				return nil, fmt.Errorf("journal extension does not bind the discovered seal and expiry")
			}
			job.Extension = &extension
			job.ExtensionSHA256 = digest
			previousExpiry, previousDigest = extension.ExpiresAt, digest
		}
		if !job.EffectiveExpiresAt().After(store.now()) {
			job.State = StateExpired
		}
		completionBytes, found, err := store.loadOptionalQuorum(ctx, CompletionHandle(jobID))
		if err != nil {
			return nil, fmt.Errorf("load journal %s completion: %w", jobID, err)
		}
		if found {
			var completion Completion
			if _, err := openObject(completionBytes, store.Key, seal.Header, "completion", 0, &completion); err != nil {
				return nil, err
			}
			if completion.State != StateCommitted || completion.SealSHA256 != job.SealSHA256 {
				return nil, fmt.Errorf("journal completion does not bind the discovered seal")
			}
			job.State, job.Completion = StateCommitted, &completion
		}
		if job.State != StateCommitted {
			abandonmentBytes, found, err := store.loadOptionalQuorum(ctx, AbandonmentHandle(jobID))
			if err != nil {
				return nil, fmt.Errorf("load journal %s abandonment: %w", jobID, err)
			}
			if found {
				var abandonment Abandonment
				if _, err := openObject(abandonmentBytes, store.Key, seal.Header, "abandonment", 0, &abandonment); err != nil {
					return nil, err
				}
				if abandonment.State != StateAbandoned || abandonment.SealSHA256 != job.SealSHA256 {
					return nil, fmt.Errorf("journal abandonment does not bind the discovered seal")
				}
				job.State, job.Abandonment = StateAbandoned, &abandonment
			}
		}
		if job.State != StateCommitted && job.State != StateAbandoned {
			rejectionBytes, found, err := store.loadOptionalQuorum(ctx, RejectionHandle(jobID))
			if err != nil {
				return nil, fmt.Errorf("load journal %s rejection: %w", jobID, err)
			}
			if found {
				var rejection Rejection
				if _, err := openObject(rejectionBytes, store.Key, seal.Header, "rejection", 0, &rejection); err != nil {
					return nil, err
				}
				if rejection.State != StateRejected || rejection.SealSHA256 != job.SealSHA256 {
					return nil, fmt.Errorf("journal rejection does not bind the discovered seal")
				}
				job.State, job.Rejection = StateRejected, &rejection
			}
		}
		jobs = append(jobs, job)
	}
	sort.Slice(jobs, func(i, j int) bool { return jobs[i].Header.CreatedAt.Before(jobs[j].Header.CreatedAt) })
	return jobs, nil
}

func (store Store) VerifyJob(ctx context.Context, job Job) ([]Segment, error) {
	if job.State != StateSealedPending && job.State != StateCommitted && job.State != StateExpired && job.State != StateAbandoned &&
		job.State != StateRejected {
		return nil, fmt.Errorf("journal %s is not a sealed reachability root", job.Header.JobID)
	}
	if len(job.Seal.SegmentSHA256) == 0 || job.Seal.Header != job.Header {
		return nil, fmt.Errorf("journal seal header mismatch")
	}
	segments := make([]Segment, len(job.Seal.SegmentSHA256))
	previous := ""
	for index, expectedDigest := range job.Seal.SegmentSHA256 {
		sequence := uint64(index + 1)
		authoritative, err := store.loadQuorum(ctx, SegmentHandle(job.Header.JobID, sequence))
		if err != nil {
			return nil, fmt.Errorf("load sealed journal segment %d: %w", sequence, err)
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
	return backend.Handle{Type: backend.StagingFile, Name: fmt.Sprintf("%s--segment--%020d", jobID, sequence)}
}

func SealHandle(jobID string) backend.Handle {
	return backend.Handle{Type: backend.StagingFile, Name: jobID + "--seal"}
}

func CompletionHandle(jobID string) backend.Handle {
	return backend.Handle{Type: backend.StagingFile, Name: jobID + "--completion"}
}

func AbandonmentHandle(jobID string) backend.Handle {
	return backend.Handle{Type: backend.StagingFile, Name: jobID + "--abandonment"}
}

func RejectionHandle(jobID string) backend.Handle {
	return backend.Handle{Type: backend.StagingFile, Name: jobID + "--rejection"}
}

func ExtensionHandle(jobID string, generation uint64) backend.Handle {
	return backend.Handle{Type: backend.StagingFile, Name: fmt.Sprintf("%s--extension--%020d", jobID, generation)}
}

func CheckQuota(quota Quota, activeJobs, principalJobs, stagedBytes uint64, oldest time.Time, additional uint64, now time.Time) error {
	if quota.MaxJobs > 0 && activeJobs >= quota.MaxJobs || quota.MaxPerPrincipal > 0 && principalJobs >= quota.MaxPerPrincipal ||
		quota.MaxBytes > 0 && stagedBytes+additional > quota.MaxBytes ||
		quota.MaxAge > 0 && !oldest.IsZero() && now.Sub(oldest) > quota.MaxAge {
		return fmt.Errorf("deferred staging quota exceeded")
	}
	return nil
}

func validatePack(pack Pack, policy Policy) error {
	if pack.Size <= 0 || pack.PayloadSize+pack.HeaderSize != uint64(pack.Size) || pack.BlobCount == 0 {
		return fmt.Errorf("invalid staged pack size accounting")
	}
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
	if object.Format != Format || object.RepositoryID != header.RepositoryID || object.JobID != header.JobID || object.Kind != kind ||
		object.Sequence != sequence {
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
