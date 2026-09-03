package staging

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

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

func BuildDaemonCommitPlan(ctx context.Context, store schemaReader, segments []Segment) (DaemonCommitPlan, error) {
	if store == nil {
		return DaemonCommitPlan{}, fmt.Errorf("schema store is required")
	}
	plan := DaemonCommitPlan{}
	planned := make(map[string][]byte)
	for _, segment := range segments {
		for _, record := range segment.Records {
			if record.Kind != SchemaFactKind {
				return DaemonCommitPlan{}, Reject(fmt.Errorf("unsupported journal record kind %q", record.Kind))
			}
			var fact SchemaFact
			decoder := json.NewDecoder(bytes.NewReader(record.Payload))
			decoder.DisallowUnknownFields()
			if err := decoder.Decode(&fact); err != nil {
				return DaemonCommitPlan{}, Reject(fmt.Errorf("decode schema fact: %w", err))
			}
			if err := validateSchemaFact(fact.Key, fact.Value); err != nil {
				return DaemonCommitPlan{}, Reject(err)
			}
			parsed, _ := schema.ParseKey(fact.Key)
			key := string(fact.Key)
			if parsed.Kind == schema.KeyBlob {
				value, ok := planned[key]
				if !ok {
					var found bool
					var err error
					value, found, err = store.Get(ctx, fact.Key)
					if err != nil {
						return DaemonCommitPlan{}, Retryable(err)
					}
					if !found {
						value = nil
					}
				}
				merged, err := mergeBlobRecords(value, fact.Value)
				if err != nil {
					return DaemonCommitPlan{}, Reject(err)
				}
				planned[key] = merged
				continue
			}
			if existing, ok := planned[key]; ok {
				if !bytes.Equal(existing, fact.Value) {
					return DaemonCommitPlan{}, Reject(fmt.Errorf("conflicting facts for schema key %x", fact.Key))
				}
				continue
			}
			existing, found, err := store.Get(ctx, fact.Key)
			if err != nil {
				return DaemonCommitPlan{}, Retryable(err)
			}
			if found && !bytes.Equal(existing, fact.Value) {
				return DaemonCommitPlan{}, Reject(fmt.Errorf("journal conflicts with authoritative schema key %x", fact.Key))
			}
			planned[key] = fact.Value
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
