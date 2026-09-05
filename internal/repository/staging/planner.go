package staging

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"sort"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
)

const SchemaFactKind = "schema-fact-v1"

type SchemaFact struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

type schemaReader interface {
	Get(context.Context, []byte) ([]byte, bool, error)
}

func SchemaFactRecord(key, value []byte) (Record, error) {
	if err := validateSchemaFact(key, value); err != nil {
		return Record{}, err
	}
	payload, err := json.Marshal(SchemaFact{Key: key, Value: value})
	if err != nil {
		return Record{}, err
	}
	return Record{Kind: SchemaFactKind, Payload: payload}, nil
}

//nolint:funlen,gocognit,gocyclo // Existing domain flow is an explicit complexity exception; new code remains gated.
func BuildDaemonCommitPlan(ctx context.Context, store schemaReader, segments []Segment) (DaemonCommitPlan, error) {
	if store == nil {
		return DaemonCommitPlan{}, fmt.Errorf("schema store is required")
	}
	plan := DaemonCommitPlan{}
	planned := make(map[string][]byte)
	addFact := func(fact SchemaFact) error {
		if err := validateSchemaFact(fact.Key, fact.Value); err != nil {
			return Reject(err)
		}
		parsed, _ := schema.ParseKey(fact.Key)
		key := string(fact.Key)
		//nolint:nestif // Existing domain flow is an explicit complexity exception; new code remains gated.
		if parsed.Kind == schema.KeyBlob {
			value, ok := planned[key]
			if !ok {
				var found bool
				var err error
				value, found, err = store.Get(ctx, fact.Key)
				if err != nil {
					return Retryable(err)
				}
				if !found {
					value = nil
				}
			}
			merged, err := mergeBlobRecords(value, fact.Value)
			if err != nil {
				return Reject(err)
			}
			planned[key] = merged
			return nil
		}
		if existing, ok := planned[key]; ok {
			if !bytes.Equal(existing, fact.Value) {
				return Reject(fmt.Errorf("conflicting facts for schema key %x", fact.Key))
			}
			return nil
		}
		existing, found, err := store.Get(ctx, fact.Key)
		if err != nil {
			return Retryable(err)
		}
		if found && !bytes.Equal(existing, fact.Value) {
			return Reject(fmt.Errorf("journal conflicts with authoritative schema key %x", fact.Key))
		}
		planned[key] = fact.Value
		return nil
	}
	for _, segment := range segments {
		for _, pack := range segment.Packs {
			facts, err := schemaFactsForPack(pack, segment.Header.CreatedAt)
			if err != nil {
				return DaemonCommitPlan{}, Reject(err)
			}
			for _, fact := range facts {
				if err := addFact(fact); err != nil {
					return DaemonCommitPlan{}, err
				}
			}
		}
		for _, record := range segment.Records {
			if record.Kind == "crawl-observation-v1" {
				var observation json.RawMessage
				decoder := json.NewDecoder(bytes.NewReader(record.Payload))
				decoder.DisallowUnknownFields()
				if err := decoder.Decode(&observation); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
					return DaemonCommitPlan{}, Reject(fmt.Errorf("decode deferred crawl observation"))
				}
				plan.Observations = append(plan.Observations, append(json.RawMessage(nil), observation...))
				continue
			}
			if record.Kind == "prospective-snapshot-v1" {
				if len(record.Payload) == 0 || !json.Valid(record.Payload) {
					return DaemonCommitPlan{}, Reject(fmt.Errorf("invalid prospective snapshot JSON"))
				}
				digest := schema.ID(sha256.Sum256(record.Payload))
				snapshotID := hex.EncodeToString(digest[:])
				if plan.SnapshotID != "" && plan.SnapshotID != snapshotID {
					return DaemonCommitPlan{}, Reject(fmt.Errorf("journal contains multiple prospective snapshots"))
				}
				plan.SnapshotID = snapshotID
				plan.SnapshotJSON = append([]byte(nil), record.Payload...)
				continue
			}
			if record.Kind == "blob-fact-v1" {
				fact, err := schemaFactForBlob(record.Payload)
				if err != nil {
					return DaemonCommitPlan{}, Reject(err)
				}
				if err := addFact(fact); err != nil {
					return DaemonCommitPlan{}, err
				}
				continue
			}
			if record.Kind != SchemaFactKind {
				return DaemonCommitPlan{}, Reject(fmt.Errorf("unsupported journal record kind %q", record.Kind))
			}
			var fact SchemaFact
			decoder := json.NewDecoder(bytes.NewReader(record.Payload))
			decoder.DisallowUnknownFields()
			if err := decoder.Decode(&fact); err != nil {
				return DaemonCommitPlan{}, Reject(fmt.Errorf("decode schema fact: %w", err))
			}
			if err := addFact(fact); err != nil {
				return DaemonCommitPlan{}, err
			}
			parsed, _ := schema.ParseKey(fact.Key)
			if parsed.Kind == schema.KeySnapshot {
				snapshotID := hex.EncodeToString(parsed.ID[:])
				if plan.SnapshotID != "" && plan.SnapshotID != snapshotID {
					return DaemonCommitPlan{}, Reject(fmt.Errorf("journal contains multiple prospective snapshots"))
				}
				plan.SnapshotID = snapshotID
			}
		}
	}
	if plan.SnapshotID == "" {
		return DaemonCommitPlan{}, Reject(fmt.Errorf("journal has no prospective snapshot fact"))
	}
	if len(plan.Observations) == 0 {
		return DaemonCommitPlan{}, Reject(fmt.Errorf("journal has no deferred crawl observations"))
	}
	keys := make([]string, 0, len(planned))
	for key := range planned {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		plan.Puts = append(plan.Puts, daemon.Mutation{Key: []byte(key), Value: planned[key]})
	}
	return plan, nil
}

func schemaFactForBlob(payload []byte) (SchemaFact, error) {
	var fact BlobFact
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&fact); err != nil {
		return SchemaFact{}, fmt.Errorf("decode blob fact: %w", err)
	}
	blobID, err := schemaID(fact.ID)
	if err != nil {
		return SchemaFact{}, err
	}
	packID, err := schemaID(fact.PackID)
	if err != nil {
		return SchemaFact{}, err
	}
	blobType := schema.BlobData
	if fact.Type == "tree" {
		blobType = schema.BlobTree
	} else if fact.Type != "data" {
		return SchemaFact{}, fmt.Errorf("invalid blob fact type %q", fact.Type)
	}
	if fact.Length > uint(^uint32(0)) || fact.UncompressedLength > uint(^uint32(0)) {
		return SchemaFact{}, fmt.Errorf("blob fact length overflow")
	}
	value,
		err := (schema.BlobRecord{Locations: []schema.BlobLocation{{PackID: packID,
		Offset:           uint64(fact.Offset),
		Length:           uint32(fact.Length),
		UncompressedSize: uint32(fact.UncompressedLength),
		Type:             blobType}}}).MarshalBinary()
	return SchemaFact{Key: schema.BlobKey(blobID), Value: value}, err
}

func schemaFactsForPack(pack Pack, createdAt time.Time) ([]SchemaFact, error) {
	packID, err := schemaID(pack.ID)
	if err != nil {
		return nil, err
	}
	packType := schema.PackData
	if pack.Type == "tree" {
		packType = schema.PackTree
	} else if pack.Type != "data" {
		return nil, fmt.Errorf("invalid pack type %q", pack.Type)
	}
	packValue,
		err := (schema.PackRecord{Type: packType,
		PhysicalSize:      uint64(pack.Size),
		PayloadSize:       pack.PayloadSize,
		HeaderSize:        pack.HeaderSize,
		BlobCount:         pack.BlobCount,
		PhysicalSizeKnown: true,
		CreationTime:      createdAt.Unix(),
		CreationTimeKnown: true,
		Lifecycle:         schema.PackPublished,
		Tier:              schema.TierUnknown,
		RetentionSource:   schema.RetentionUnknown}).MarshalBinary()
	if err != nil {
		return nil, err
	}
	facts := []SchemaFact{{Key: schema.PackKey(packID), Value: packValue}}
	for _, placement := range pack.Placements {
		backendID := backendHash(placement.BackendID)
		placementValue,
			err := (schema.PlacementRecord{State: schema.PlacementLive,
			PlacedAt:           createdAt.Unix(),
			PlacementTimeKnown: true,
			Bytes:              uint64(placement.Size),
			RetentionSource:    schema.RetentionUnknown,
			LastVerifiedAt:     createdAt.Unix()}).MarshalBinary()
		if err != nil {
			return nil, err
		}
		backendValue, err := (schema.BackendPackRecord{State: schema.PlacementLive, Bytes: uint64(placement.Size), PlacedAt: createdAt.Unix()}).MarshalBinary()
		if err != nil {
			return nil, err
		}
		facts = append(
			facts,
			SchemaFact{Key: schema.PackPlacementKey(packID, backendID), Value: placementValue},
			SchemaFact{Key: schema.BackendPackKey(backendID, packID), Value: backendValue},
		)
	}
	return facts, nil
}

func schemaID(value string) (schema.ID, error) {
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != sha256.Size {
		return schema.ID{}, fmt.Errorf("invalid journal object ID %q", value)
	}
	var id schema.ID
	copy(id[:], decoded)
	return id, nil
}

func backendHash(value string) uint64 {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(value)) // hash.Hash writes are specified to return a nil error.
	if result := hash.Sum64(); result != 0 {
		return result
	}
	return 1
}

func validateSchemaFact(key, value []byte) error {
	parsed, err := schema.ParseKey(key)
	if err != nil {
		return fmt.Errorf("invalid journal schema key: %w", err)
	}
	switch parsed.Kind {
	case schema.KeyBlob, schema.KeyPack, schema.KeyPackPlacement, schema.KeyBackendPack,
		schema.KeySnapshot, schema.KeyContentManifest, schema.KeyInodeRevision,
		schema.KeyDirectoryRevision, schema.KeyCrawlDebt:
	default:
		return fmt.Errorf("journal may not provide mutable or derived schema key kind %d", parsed.Kind)
	}
	if err := schema.ValidateValue(key, value); err != nil {
		return fmt.Errorf("invalid journal schema value: %w", err)
	}
	return nil
}

func mergeBlobRecords(current, addition []byte) ([]byte, error) {
	added, err := schema.UnmarshalBlobRecord(addition)
	if err != nil {
		return nil, err
	}
	merged := added
	if len(current) > 0 {
		merged, err = schema.UnmarshalBlobRecord(current)
		if err != nil {
			return nil, err
		}
		for _, location := range added.Locations {
			found := false
			for _, existing := range merged.Locations {
				if existing == location {
					found = true
					break
				}
			}
			if !found {
				merged.Locations = append(merged.Locations, location)
			}
		}
	}
	sort.Slice(merged.Locations, func(i, j int) bool {
		left, right := merged.Locations[i], merged.Locations[j]
		if left.PackID != right.PackID {
			return bytes.Compare(left.PackID[:], right.PackID[:]) < 0
		}
		if left.Offset != right.Offset {
			return left.Offset < right.Offset
		}
		if left.Length != right.Length {
			return left.Length < right.Length
		}
		if left.UncompressedSize != right.UncompressedSize {
			return left.UncompressedSize < right.UncompressedSize
		}
		return left.Type < right.Type
	})
	return merged.MarshalBinary()
}
