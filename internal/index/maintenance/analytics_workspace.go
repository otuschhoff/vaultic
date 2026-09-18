package maintenance

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"

	"github.com/otuschhoff/vaultic/internal/index/schema"
)

const analyticsWorkspaceSpools = 4

type analyticsCheckWorkspace struct {
	dictionaries *checkKVSpool
	aggregates   *checkKVSpool
	summaries    *checkKVSpool
	gdpr         *checkKVSpool
	sequence     uint64
}

func newAnalyticsCheckWorkspace(
	ctx context.Context,
	scratch *checkScratch,
	memoryBytes uint64,
) (*analyticsCheckWorkspace, error) {
	spoolMemory := memoryBytes / analyticsWorkspaceSpools
	if spoolMemory < schema.MaxPathIndexPathBytes+1024 {
		return nil, fmt.Errorf("checker memory limit is too small for bounded analytics records")
	}
	workspace := &analyticsCheckWorkspace{}
	var err error
	workspace.dictionaries, err = newCheckKVSpool(ctx, scratch, spoolMemory, 32)
	if err == nil {
		workspace.aggregates, err = newCheckKVSpool(ctx, scratch, spoolMemory, 32)
	}
	if err == nil {
		workspace.summaries, err = newCheckKVSpool(ctx, scratch, spoolMemory, 32)
	}
	if err == nil {
		workspace.gdpr, err = newCheckKVSpool(ctx, scratch, spoolMemory, 32)
	}
	if err != nil {
		_ = workspace.close()
		return nil, err
	}
	return workspace, nil
}

func (workspace *analyticsCheckWorkspace) AddDictionary(kind schema.AnalyticsDictionaryKind, id uint32, value string) error {
	key := append([]byte{byte(kind)}, []byte(value)...)
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], id)
	return workspace.dictionaries.add(key, encoded[:], uint64(id))
}

func (workspace *analyticsCheckWorkspace) ForEachDictionary(
	visit func(schema.AnalyticsDictionaryKind, uint32, string) error,
) (err error) {
	iterator, err := workspace.dictionaries.iterator()
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, iterator.close()) }()
	for {
		record, found, err := iterator.next()
		if err != nil || !found {
			return err
		}
		if len(record.key) < 2 || len(record.value) != 4 {
			return fmt.Errorf("invalid analytics dictionary workspace record")
		}
		if err := visit(
			schema.AnalyticsDictionaryKind(record.key[0]), binary.BigEndian.Uint32(record.value), string(record.key[1:]),
		); err != nil {
			return err
		}
	}
}

func (workspace *analyticsCheckWorkspace) AddAggregate(key []byte, record schema.AnalyticsAggregateRecord) error {
	value, err := record.MarshalBinary()
	if err != nil {
		return err
	}
	return workspace.aggregates.add(key, value, 0)
}

func (workspace *analyticsCheckWorkspace) AddSummary(key []byte, record schema.AnalyticsSummaryRecord) error {
	value, err := record.MarshalBinary()
	if err != nil {
		return err
	}
	return workspace.summaries.add(key, value, 0)
}

func (workspace *analyticsCheckWorkspace) AddGDPR(key, value []byte) error {
	workspace.sequence++
	if workspace.sequence == 0 {
		return fmt.Errorf("analytics workspace sequence overflow")
	}
	return workspace.gdpr.add(key, value, workspace.sequence)
}

func (workspace *analyticsCheckWorkspace) ForEachAggregate(
	visit func([]byte, schema.AnalyticsAggregateRecord) error,
) (err error) {
	iterator, err := workspace.aggregates.iterator()
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, iterator.close()) }()
	var key []byte
	var total schema.AnalyticsAggregateRecord
	flush := func() error {
		if key == nil {
			return nil
		}
		return visit(key, total)
	}
	for {
		record, found, err := iterator.next()
		if err != nil {
			return err
		}
		if !found {
			return flush()
		}
		if key != nil && !bytes.Equal(key, record.key) {
			if err := flush(); err != nil {
				return err
			}
			total = schema.AnalyticsAggregateRecord{}
		}
		key = append(key[:0], record.key...)
		contribution, err := schema.UnmarshalAnalyticsAggregateRecord(record.value)
		if err != nil {
			return err
		}
		if total.BytesAdded, err = checkedAdd(total.BytesAdded, contribution.BytesAdded); err != nil {
			return err
		}
		if total.BytesDeleted, err = checkedAdd(total.BytesDeleted, contribution.BytesDeleted); err != nil {
			return err
		}
		if total.FilesAdded, err = checkedAdd(total.FilesAdded, contribution.FilesAdded); err != nil {
			return err
		}
		if total.FilesDeleted, err = checkedAdd(total.FilesDeleted, contribution.FilesDeleted); err != nil {
			return err
		}
	}
}

func (workspace *analyticsCheckWorkspace) ForEachSummary(
	visit func([]byte, schema.AnalyticsSummaryRecord) error,
) (err error) {
	iterator, err := workspace.summaries.iterator()
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, iterator.close()) }()
	var key []byte
	var total schema.AnalyticsSummaryRecord
	flush := func() error {
		if key == nil {
			return nil
		}
		return visit(key, total)
	}
	for {
		record, found, err := iterator.next()
		if err != nil {
			return err
		}
		if !found {
			return flush()
		}
		if key != nil && !bytes.Equal(key, record.key) {
			if err := flush(); err != nil {
				return err
			}
			total = schema.AnalyticsSummaryRecord{}
		}
		key = append(key[:0], record.key...)
		contribution, err := schema.UnmarshalAnalyticsSummaryRecord(record.value)
		if err != nil {
			return err
		}
		if total.ActiveBytes, err = checkedAdd(total.ActiveBytes, contribution.ActiveBytes); err != nil {
			return err
		}
		if total.ActiveFiles, err = checkedAdd(total.ActiveFiles, contribution.ActiveFiles); err != nil {
			return err
		}
		if total.UniqueBlobCount, err = checkedAdd(total.UniqueBlobCount, contribution.UniqueBlobCount); err != nil {
			return err
		}
		if total.UniqueBlobBytes, err = checkedAdd(total.UniqueBlobBytes, contribution.UniqueBlobBytes); err != nil {
			return err
		}
	}
}

func (workspace *analyticsCheckWorkspace) ForEachGDPR(visit func([]byte, []byte) error) (err error) {
	iterator, err := workspace.gdpr.iterator()
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, iterator.close()) }()
	var latest checkKVRecord
	flush := func() error {
		if latest.key == nil {
			return nil
		}
		return visit(latest.key, latest.value)
	}
	for {
		record, found, err := iterator.next()
		if err != nil {
			return err
		}
		if !found {
			return flush()
		}
		if latest.key != nil && !bytes.Equal(latest.key, record.key) {
			if err := flush(); err != nil {
				return err
			}
		}
		latest = record
	}
}

func checkedAdd(left, right uint64) (uint64, error) {
	result, carry := bits.Add64(left, right, 0)
	if carry != 0 {
		return 0, fmt.Errorf("analytics consistency reduction overflow")
	}
	return result, nil
}

func (workspace *analyticsCheckWorkspace) close() error {
	var first error
	for _, spool := range []*checkKVSpool{workspace.dictionaries, workspace.aggregates, workspace.summaries, workspace.gdpr} {
		if spool != nil {
			if err := spool.close(); err != nil && first == nil {
				first = err
			}
		}
	}
	return first
}
