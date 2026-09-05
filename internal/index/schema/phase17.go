package schema

import (
	"bytes"
	"fmt"
)

type VerificationLevel byte

const (
	VerificationHeader VerificationLevel = iota + 1
	VerificationChecksum
	VerificationFull
)

func validVerificationLevel(value VerificationLevel) bool {
	return value >= VerificationHeader && value <= VerificationFull
}

type VerificationResult byte

const (
	VerificationUnknown VerificationResult = iota
	VerificationHealthy
	VerificationOperationalError
	VerificationIntegrityError
)

func validVerificationResult(value VerificationResult) bool {
	return value <= VerificationIntegrityError
}

type VerificationClassification byte

const (
	VerificationNoError VerificationClassification = iota
	VerificationMissing
	VerificationSizeMismatch
	VerificationChecksumMismatch
	VerificationHeaderAuthentication
	VerificationPayloadDecrypt
	VerificationDecompression
	VerificationWarmupTimeout
	VerificationTransport
	VerificationCancelled
)

func validVerificationClassification(value VerificationClassification) bool {
	return value <= VerificationCancelled
}

func (value VerificationClassification) IsIntegrity() bool {
	return value >= VerificationMissing && value <= VerificationDecompression
}

type VerificationEventType byte

const (
	VerificationDetected VerificationEventType = iota + 1
	VerificationChanged
	VerificationResolved
)

func validVerificationEventType(value VerificationEventType) bool {
	return value >= VerificationDetected && value <= VerificationResolved
}

type VerificationStateRecord struct {
	LastAttemptAt       int64
	LastAttemptLevel    VerificationLevel
	HeaderVerifiedAt    int64
	ChecksumVerifiedAt  int64
	FullVerifiedAt      int64
	Result              VerificationResult
	OpenFindingID       ID
	FindingLevel        VerificationLevel
	Classification      VerificationClassification
	FirstErrorAt        int64
	LastErrorAt         int64
	ConsecutiveFailures uint64
	LastRunID           ID
}

func (record VerificationStateRecord) validate() error {
	if record.LastAttemptAt < 0 || record.HeaderVerifiedAt < 0 || record.ChecksumVerifiedAt < 0 ||
		record.FullVerifiedAt < 0 ||
		record.FirstErrorAt < 0 ||
		record.LastErrorAt < 0 ||
		!validVerificationResult(record.Result) {
		return fmt.Errorf("%w: invalid verification state", ErrMalformed)
	}
	if err := record.validateAttempt(); err != nil {
		return err
	}
	if record.FullVerifiedAt > 0 &&
		(record.ChecksumVerifiedAt < record.FullVerifiedAt || record.HeaderVerifiedAt < record.FullVerifiedAt) {
		return fmt.Errorf("%w: full verification does not imply weaker levels", ErrMalformed)
	}
	if record.ChecksumVerifiedAt > 0 && record.HeaderVerifiedAt < record.ChecksumVerifiedAt {
		return fmt.Errorf("%w: checksum verification does not imply header", ErrMalformed)
	}
	return record.validateFinding()
}

func (record VerificationStateRecord) validateAttempt() error {
	if record.LastAttemptAt == 0 {
		if record.LastAttemptLevel != 0 || record.LastRunID != (ID{}) {
			return fmt.Errorf("%w: verification attempt metadata without attempt", ErrMalformed)
		}
		return nil
	}
	if !validVerificationLevel(record.LastAttemptLevel) || record.LastRunID == (ID{}) {
		return fmt.Errorf("%w: incomplete verification attempt", ErrMalformed)
	}
	return nil
}

func (record VerificationStateRecord) validateFinding() error {
	hasFinding := record.OpenFindingID != (ID{})
	if hasFinding != (record.Result == VerificationOperationalError || record.Result == VerificationIntegrityError) {
		return fmt.Errorf("%w: finding and result disagree", ErrMalformed)
	}
	if !hasFinding {
		if record.FindingLevel != 0 ||
			record.Classification != VerificationNoError ||
			record.FirstErrorAt != 0 ||
			record.LastErrorAt != 0 ||
			record.ConsecutiveFailures != 0 {
			return fmt.Errorf("%w: stale verification finding metadata", ErrMalformed)
		}
		return nil
	}
	if !validVerificationLevel(record.FindingLevel) || record.Classification == VerificationNoError ||
		!validVerificationClassification(record.Classification) || record.FirstErrorAt == 0 ||
		record.LastErrorAt < record.FirstErrorAt || record.ConsecutiveFailures == 0 {
		return fmt.Errorf("%w: incomplete verification finding", ErrMalformed)
	}
	if (record.Result == VerificationIntegrityError) != record.Classification.IsIntegrity() {
		return fmt.Errorf("%w: verification finding class and result disagree", ErrMalformed)
	}
	return nil
}

func (record VerificationStateRecord) MarshalBinary() ([]byte, error) {
	if err := record.validate(); err != nil {
		return nil, err
	}
	e := newEncoder()
	e.i64(record.LastAttemptAt)
	e.u8(byte(record.LastAttemptLevel))
	e.i64(record.HeaderVerifiedAt)
	e.i64(record.ChecksumVerifiedAt)
	e.i64(record.FullVerifiedAt)
	e.u8(byte(record.Result))
	e.id(record.OpenFindingID)
	e.u8(byte(record.FindingLevel))
	e.u8(byte(record.Classification))
	e.i64(record.FirstErrorAt)
	e.i64(record.LastErrorAt)
	e.u64(record.ConsecutiveFailures)
	e.id(record.LastRunID)
	return e.finish()
}

func UnmarshalVerificationStateRecord(data []byte) (VerificationStateRecord, error) {
	d, err := newDecoder(data)
	if err != nil {
		return VerificationStateRecord{}, err
	}
	var record VerificationStateRecord
	if record.LastAttemptAt, err = d.i64(); err != nil {
		return record, err
	}
	value, err := d.u8()
	record.LastAttemptLevel = VerificationLevel(value)
	if err != nil {
		return record, err
	}
	if record.HeaderVerifiedAt, err = d.i64(); err != nil {
		return record, err
	}
	if record.ChecksumVerifiedAt, err = d.i64(); err != nil {
		return record, err
	}
	if record.FullVerifiedAt, err = d.i64(); err != nil {
		return record, err
	}
	value, err = d.u8()
	record.Result = VerificationResult(value)
	if err != nil {
		return record, err
	}
	if record.OpenFindingID, err = d.id(); err != nil {
		return record, err
	}
	value, err = d.u8()
	record.FindingLevel = VerificationLevel(value)
	if err != nil {
		return record, err
	}
	value, err = d.u8()
	record.Classification = VerificationClassification(value)
	if err != nil {
		return record, err
	}
	if record.FirstErrorAt, err = d.i64(); err != nil {
		return record, err
	}
	if record.LastErrorAt, err = d.i64(); err != nil {
		return record, err
	}
	if record.ConsecutiveFailures, err = d.u64(); err != nil {
		return record, err
	}
	if record.LastRunID, err = d.id(); err != nil {
		return record, err
	}
	if err = d.done(); err != nil {
		return VerificationStateRecord{}, err
	}
	return record, record.validate()
}

type VerificationEventRecord struct {
	Type           VerificationEventType
	FindingID      ID
	RunID          ID
	Level          VerificationLevel
	Classification VerificationClassification
	FirstDetected  int64
	LastDetected   int64
	Occurrences    uint64
	Expected       string
	Observed       string
	Resolution     string
}

func (record VerificationEventRecord) validate() error {
	if !validVerificationEventType(record.Type) || record.FindingID == (ID{}) || record.RunID == (ID{}) ||
		!validVerificationLevel(record.Level) ||
		!validVerificationClassification(record.Classification) ||
		record.FirstDetected <= 0 ||
		record.LastDetected < record.FirstDetected ||
		record.Occurrences == 0 {
		return fmt.Errorf("%w: invalid verification event", ErrMalformed)
	}
	if record.Type == VerificationResolved {
		if record.Resolution == "" {
			return fmt.Errorf("%w: resolution event without reason", ErrMalformed)
		}
	} else if record.Classification == VerificationNoError || record.Resolution != "" {
		return fmt.Errorf("%w: invalid unresolved verification event", ErrMalformed)
	}
	return nil
}

func (record VerificationEventRecord) MarshalBinary() ([]byte, error) {
	if err := record.validate(); err != nil {
		return nil, err
	}
	e := newEncoder()
	e.u8(byte(record.Type))
	e.id(record.FindingID)
	e.id(record.RunID)
	e.u8(byte(record.Level))
	e.u8(byte(record.Classification))
	e.i64(record.FirstDetected)
	e.i64(record.LastDetected)
	e.u64(record.Occurrences)
	if err := e.string(record.Expected); err != nil {
		return nil, err
	}
	if err := e.string(record.Observed); err != nil {
		return nil, err
	}
	if err := e.string(record.Resolution); err != nil {
		return nil, err
	}
	return e.finish()
}

func UnmarshalVerificationEventRecord(data []byte) (VerificationEventRecord, error) {
	d, err := newDecoder(data)
	if err != nil {
		return VerificationEventRecord{}, err
	}
	var record VerificationEventRecord
	value, err := d.u8()
	record.Type = VerificationEventType(value)
	if err != nil {
		return record, err
	}
	if record.FindingID, err = d.id(); err != nil {
		return record, err
	}
	if record.RunID, err = d.id(); err != nil {
		return record, err
	}
	value, err = d.u8()
	record.Level = VerificationLevel(value)
	if err != nil {
		return record, err
	}
	value, err = d.u8()
	record.Classification = VerificationClassification(value)
	if err != nil {
		return record, err
	}
	if record.FirstDetected, err = d.i64(); err != nil {
		return record, err
	}
	if record.LastDetected, err = d.i64(); err != nil {
		return record, err
	}
	if record.Occurrences, err = d.u64(); err != nil {
		return record, err
	}
	if record.Expected, err = d.string(); err != nil {
		return record, err
	}
	if record.Observed, err = d.string(); err != nil {
		return record, err
	}
	if record.Resolution, err = d.string(); err != nil {
		return record, err
	}
	if err = d.done(); err != nil {
		return VerificationEventRecord{}, err
	}
	return record, record.validate()
}

type UIDExclusionPolicyRecord struct {
	Excluded  bool
	UpdatedAt int64
	RunID     ID
	Reason    string
}

func (record UIDExclusionPolicyRecord) MarshalBinary() ([]byte, error) {
	if record.UpdatedAt <= 0 || record.RunID == (ID{}) {
		return nil, fmt.Errorf("%w: invalid UID exclusion policy", ErrMalformed)
	}
	e := newEncoder()
	e.bool(record.Excluded)
	e.i64(record.UpdatedAt)
	e.id(record.RunID)
	if err := e.string(record.Reason); err != nil {
		return nil, err
	}
	return e.finish()
}

func UnmarshalUIDExclusionPolicyRecord(data []byte) (UIDExclusionPolicyRecord, error) {
	d, err := newDecoder(data)
	if err != nil {
		return UIDExclusionPolicyRecord{}, err
	}
	var record UIDExclusionPolicyRecord
	if record.Excluded, err = d.bool(); err != nil {
		return record, err
	}
	if record.UpdatedAt, err = d.i64(); err != nil {
		return record, err
	}
	if record.RunID, err = d.id(); err != nil {
		return record, err
	}
	if record.Reason, err = d.string(); err != nil {
		return record, err
	}
	if err = d.done(); err != nil {
		return UIDExclusionPolicyRecord{}, err
	}
	if record.UpdatedAt <= 0 || record.RunID == (ID{}) {
		return record, fmt.Errorf("%w: invalid UID exclusion policy", ErrMalformed)
	}
	return record, nil
}

type DeletionSchedule struct {
	PackID      ID
	Backend     uint64
	DeleteAfter int64
}

type DeletionCertificateRecord struct {
	UID                   uint32
	ExecutedAt            int64
	RunID                 ID
	PurgedReferenceHashes []ID
	PendingDeletion       []DeletionSchedule
	SigningAlgorithm      string
	PublicKey             []byte
	Signature             []byte
}

func compareDeletionSchedule(left, right DeletionSchedule) int {
	if value := bytes.Compare(left.PackID[:], right.PackID[:]); value != 0 {
		return value
	}
	if left.Backend < right.Backend {
		return -1
	}
	if left.Backend > right.Backend {
		return 1
	}
	if left.DeleteAfter < right.DeleteAfter {
		return -1
	}
	if left.DeleteAfter > right.DeleteAfter {
		return 1
	}
	return 0
}

func (record DeletionCertificateRecord) marshal(includeSignature bool) ([]byte, error) {
	if record.ExecutedAt <= 0 || record.RunID == (ID{}) || record.SigningAlgorithm == "" ||
		len(record.PublicKey) == 0 ||
		(includeSignature && len(record.Signature) == 0) {
		return nil, fmt.Errorf("%w: invalid deletion certificate", ErrMalformed)
	}
	if len(record.PurgedReferenceHashes) > 1<<24 || len(record.PendingDeletion) > 1<<24 {
		return nil, fmt.Errorf("%w: deletion certificate is too large", ErrMalformed)
	}
	e := newEncoder()
	e.u32(record.UID)
	e.i64(record.ExecutedAt)
	e.id(record.RunID)
	e.u32(uint32(len(record.PurgedReferenceHashes)))
	previous := ID{}
	for index, id := range record.PurgedReferenceHashes {
		if id == (ID{}) || (index > 0 && bytes.Compare(previous[:], id[:]) >= 0) {
			return nil, fmt.Errorf("%w: deletion hashes are not uniquely sorted", ErrMalformed)
		}
		e.id(id)
		previous = id
	}
	e.u32(uint32(len(record.PendingDeletion)))
	previousSchedule := DeletionSchedule{}
	for index, item := range record.PendingDeletion {
		if item.PackID == (ID{}) || item.Backend == 0 || item.DeleteAfter < 0 ||
			(index > 0 && compareDeletionSchedule(previousSchedule, item) >= 0) {
			return nil, fmt.Errorf("%w: deletion schedules are not uniquely sorted", ErrMalformed)
		}
		e.id(item.PackID)
		e.u64(item.Backend)
		e.i64(item.DeleteAfter)
		previousSchedule = item
	}
	if err := e.string(record.SigningAlgorithm); err != nil {
		return nil, err
	}
	if err := e.bytes(record.PublicKey); err != nil {
		return nil, err
	}
	if includeSignature {
		if err := e.bytes(record.Signature); err != nil {
			return nil, err
		}
	}
	return e.finish()
}

func (record DeletionCertificateRecord) SigningBytes() ([]byte, error)  { return record.marshal(false) }
func (record DeletionCertificateRecord) MarshalBinary() ([]byte, error) { return record.marshal(true) }

func UnmarshalDeletionCertificateRecord(data []byte) (DeletionCertificateRecord, error) {
	d, err := newDecoder(data)
	if err != nil {
		return DeletionCertificateRecord{}, err
	}
	var record DeletionCertificateRecord
	if record.UID, err = d.u32(); err != nil {
		return record, err
	}
	if record.ExecutedAt, err = d.i64(); err != nil {
		return record, err
	}
	if record.RunID, err = d.id(); err != nil {
		return record, err
	}
	count, err := d.u32()
	if err != nil {
		return record, err
	}
	record.PurgedReferenceHashes = make([]ID, count)
	for index := range record.PurgedReferenceHashes {
		if record.PurgedReferenceHashes[index], err = d.id(); err != nil {
			return record, err
		}
	}
	count, err = d.u32()
	if err != nil {
		return record, err
	}
	record.PendingDeletion = make([]DeletionSchedule, count)
	for index := range record.PendingDeletion {
		if record.PendingDeletion[index].PackID, err = d.id(); err != nil {
			return record, err
		}
		if record.PendingDeletion[index].Backend, err = d.u64(); err != nil {
			return record, err
		}
		if record.PendingDeletion[index].DeleteAfter, err = d.i64(); err != nil {
			return record, err
		}
	}
	if record.SigningAlgorithm, err = d.string(); err != nil {
		return record, err
	}
	if record.PublicKey, err = d.bytes(); err != nil {
		return record, err
	}
	if record.Signature, err = d.bytes(); err != nil {
		return record, err
	}
	if err = d.done(); err != nil {
		return DeletionCertificateRecord{}, err
	}
	encoded, err := record.MarshalBinary()
	if err != nil {
		return DeletionCertificateRecord{}, err
	}
	if !bytes.Equal(encoded, data) {
		return DeletionCertificateRecord{}, fmt.Errorf("%w: non-canonical deletion certificate", ErrMalformed)
	}
	return record, nil
}
