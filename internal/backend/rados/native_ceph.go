//go:build rados

package rados

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"math"
	"strings"
	"syscall"
	"time"

	radosgo "github.com/otuschhoff/rados-go"
	"github.com/otuschhoff/vaultic/internal/backend"
)

type radosObjectStore struct {
	client     *radosgo.Client
	poolHandle radosgo.Pool
	pool       string
	opTTL      time.Duration
	cursors    nativeListCursorRegistry
}

func openNative(ctx context.Context, config Config) (driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	client, err := radosgo.New(radosgo.Config{
		Monitors:         splitMonitors(config.Monitors),
		Entity:           config.Client,
		ClusterFSID:      config.ClusterFSID,
		Key:              []byte(config.Key.Unwrap()),
		DialTimeout:      config.OperationTTL,
		HandshakeTimeout: config.OperationTTL,
		OperationTimeout: config.OperationTTL,
	})
	if err != nil {
		return nil, mapRadosGoError(err)
	}
	failed := true
	defer func() {
		if failed {
			_ = client.Close()
		}
	}()
	if err := client.Connect(ctx); err != nil {
		return nil, fmt.Errorf("connect native RADOS: %w", mapRadosGoError(err))
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	fsid := client.FSID()
	if !strings.EqualFold(fsid, config.ClusterFSID) {
		return nil, fmt.Errorf("native RADOS cluster identity %q does not match sealed identity %q", fsid, config.ClusterFSID)
	}
	pool, err := client.OpenPool(ctx, config.Pool)
	if err != nil {
		return nil, fmt.Errorf("open native RADOS pool %q: %w", config.Pool, mapRadosGoError(err))
	}
	failed = false
	native := &nativeDriver{
		raw:                 &radosObjectStore{client: client, poolHandle: pool.WithNamespace(config.Namespace), pool: config.Pool, opTTL: config.OperationTTL},
		prefix:              strings.Trim(config.Prefix, "/") + "/",
		pool:                config.Pool,
		opTTL:               config.OperationTTL,
		publicationLeaseKey: derivePublicationLeaseKey(config.Key.Unwrap(), fsid, config.Pool, config.Namespace, config.Prefix, config.Client),
		readSlots:           make(chan struct{}, maxDetachedReadOperations),
		writeSlots:          make(chan struct{}, maxDetachedWriteOperations),
	}
	native.resetOrphanGCState()
	native.startOrphanGCWorker()
	return native, nil
}

func (store *radosObjectStore) operationContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), store.opTTL)
}

func (store *radosObjectStore) stat(name string) (uint64, error) {
	ctx, cancel := store.operationContext()
	defer cancel()
	value, err := store.poolHandle.Object(name).Stat(ctx)
	return value.Size, mapRadosGoError(err)
}

func (store *radosObjectStore) statWithModTime(name string) (uint64, time.Time, bool, error) {
	ctx, cancel := store.operationContext()
	defer cancel()
	value, err := store.poolHandle.Object(name).Stat(ctx)
	if err != nil {
		return 0, time.Time{}, false, mapRadosGoError(err)
	}
	return value.Size, value.ModTime, true, nil
}

func (store *radosObjectStore) read(name string, buffer []byte, offset uint64) (int, error) {
	ctx, cancel := store.operationContext()
	defer cancel()
	data, _, err := store.poolHandle.Object(name).Read(ctx, offset, uint64(len(buffer)))
	if err != nil {
		return 0, mapRadosGoError(err)
	}
	return copy(buffer, data), nil
}

func (store *radosObjectStore) write(name string, data []byte, exclusive bool) error {
	ctx, cancel := store.operationContext()
	defer cancel()
	operation := radosgo.NewWriteOp()
	if exclusive {
		operation.Create(true)
	}
	operation.WriteFull(data)
	_, err := store.poolHandle.Object(name).ExecuteWrite(ctx, operation)
	return mapRadosGoError(err)
}

func (store *radosObjectStore) compareAndSwap(name string, expected []byte, replacement []byte, createOnly bool) (bool, error) {
	ctx, cancel := store.operationContext()
	defer cancel()
	object := store.poolHandle.Object(name)
	if createOnly {
		operation := radosgo.NewWriteOp()
		operation.Create(true)
		operation.WriteFull(replacement)
		_, err := object.ExecuteWrite(ctx, operation)
		if err == nil {
			return true, nil
		}
		if stderrors.Is(err, radosgo.ErrExists) {
			return false, nil
		}
		return false, mapRadosGoError(err)
	}
	stat, err := object.Stat(ctx)
	if stderrors.Is(err, radosgo.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, mapRadosGoError(err)
	}
	if stat.Size != uint64(len(expected)) {
		return false, nil
	}
	operation := radosgo.NewWriteOp()
	operation.AssertVersion(stat.Version)
	compareIndex := operation.CompareExtent(0, expected)
	operation.WriteFull(replacement)
	result, err := object.ExecuteWrite(ctx, operation)
	if err == nil {
		return true, nil
	}
	if stderrors.Is(err, radosgo.ErrConflict) || stderrors.Is(err, radosgo.ErrNotFound) || compareIndex < len(result.Results) && result.Results[compareIndex].Err != nil {
		return false, nil
	}
	return false, mapRadosGoError(err)
}

func (store *radosObjectStore) remove(name string) error {
	ctx, cancel := store.operationContext()
	defer cancel()
	_, err := store.poolHandle.Object(name).Remove(ctx)
	return mapRadosGoError(err)
}

func (store *radosObjectStore) listPage(ctx context.Context, prefix string, after string, limit int) ([]string, string, bool, error) {
	return store.cursors.page(ctx, prefix, after, limit, func() (nativeObjectIterator, error) {
		return &radosObjectIterator{ctx: ctx, pool: store.poolHandle, cursor: store.poolHandle.BeginObjectCursor(), index: -1}, nil
	})
}

type radosObjectIterator struct {
	ctx    context.Context
	pool   radosgo.Pool
	cursor radosgo.ObjectCursor
	values []radosgo.ObjectEntry
	index  int
	done   bool
	err    error
}

func (iterator *radosObjectIterator) Next() bool {
	for !iterator.done && iterator.err == nil {
		iterator.index++
		if iterator.index < len(iterator.values) {
			return true
		}
		if iterator.cursor.IsEnd() {
			iterator.done = true
			break
		}
		page, err := iterator.pool.ListObjects(iterator.ctx, iterator.cursor, orphanChunkScanPageLimit)
		if err != nil {
			iterator.err = mapRadosGoError(err)
			break
		}
		iterator.values, iterator.cursor, iterator.index = page.Values, page.Next, 0
		if len(iterator.values) != 0 {
			return true
		}
	}
	return false
}

func (iterator *radosObjectIterator) Value() string { return iterator.values[iterator.index].Name }
func (iterator *radosObjectIterator) Err() error    { return iterator.err }
func (iterator *radosObjectIterator) Close()        { iterator.done = true }
func (iterator *radosObjectIterator) setContext(ctx context.Context) {
	iterator.ctx = ctx
}

func (store *radosObjectStore) close() {
	store.cursors.close()
	_ = store.client.Close()
}

func (store *radosObjectStore) capacity(ctx context.Context, pool string) (backend.CapacityTelemetrySample, error) {
	if err := ctx.Err(); err != nil {
		return backend.CapacityTelemetrySample{}, err
	}
	cluster, clusterErr := store.client.ClusterStats(ctx)
	if clusterErr != nil {
		return backend.CapacityTelemetrySample{}, mapRadosGoError(clusterErr)
	}
	sample := backend.CapacityTelemetrySample{TotalRawBytes: cluster.KB * 1024, FreeRawBytes: cluster.KBAvailable * 1024, Health: "healthy"}
	if sample.FreeRawBytes > sample.TotalRawBytes {
		sample.Inconsistent = true
	}
	if health, ok := store.healthStatus(ctx); ok {
		sample.Health = health
	} else {
		sample.Inconsistent = true
	}
	if size, known := store.poolReplicaValue(ctx, pool, "size"); known {
		sample.PoolReplicaSizeKnown, sample.PoolReplicaSize = true, size
		if sample.RawAmplification < 1 {
			sample.RawAmplification = float64(size)
		}
	}
	if minSize, known := store.poolReplicaValue(ctx, pool, "min_size"); known {
		sample.PoolMinSizeKnown, sample.PoolMinSize = true, minSize
	}
	if maxAvail, quotaBytes, storedBytes, ok := store.poolSpaceFacts(ctx, pool); ok {
		sample.PoolMaxAvailBytes = maxAvail
		sample.PoolMaxAvailKnown = true
		if sample.RawAmplification >= 1 {
			sample.PoolMaxAvailRawBytes = logicalToRawHeadroom(maxAvail, sample.RawAmplification)
		}
		if quotaBytes > 0 {
			sample.PoolQuotaAvailableBytes = saturatingSub(quotaBytes, storedBytes)
			sample.PoolQuotaKnown = true
			if sample.RawAmplification >= 1 {
				sample.PoolQuotaRawBytes = logicalToRawHeadroom(sample.PoolQuotaAvailableBytes, sample.RawAmplification)
			}
		}
	}
	return sample, ctx.Err()
}

func (store *radosObjectStore) healthStatus(ctx context.Context) (string, bool) {
	raw, _, err := store.monCommand(ctx, map[string]any{"prefix": "status", "format": "json"})
	if err != nil {
		return "", false
	}
	var payload struct {
		Health struct {
			Status string `json:"status"`
		} `json:"health"`
	}
	if json.Unmarshal(raw, &payload) != nil {
		return "", false
	}
	switch strings.ToUpper(strings.TrimSpace(payload.Health.Status)) {
	case "HEALTH_OK":
		return "healthy", true
	case "HEALTH_WARN":
		return "nearfull", true
	case "HEALTH_ERR":
		return "full", true
	default:
		return "", false
	}
}

func (store *radosObjectStore) poolSpaceFacts(ctx context.Context, pool string) (uint64, uint64, uint64, bool) {
	raw, _, err := store.monCommand(ctx, map[string]any{"prefix": "df", "format": "json"})
	if err != nil {
		return 0, 0, 0, false
	}
	var payload struct {
		Pools []struct {
			Name  string `json:"name"`
			Stats struct {
				MaxAvail uint64 `json:"max_avail"`
				Stored   uint64 `json:"stored"`
			} `json:"stats"`
			Quota struct {
				MaxBytes uint64 `json:"max_bytes"`
			} `json:"quota"`
		} `json:"pools"`
	}
	if json.Unmarshal(raw, &payload) != nil {
		return 0, 0, 0, false
	}
	for _, entry := range payload.Pools {
		if entry.Name == pool {
			return entry.Stats.MaxAvail, entry.Quota.MaxBytes, entry.Stats.Stored, true
		}
	}
	return 0, 0, 0, false
}

func (store *radosObjectStore) poolReplicaValue(ctx context.Context, pool string, variable string) (uint64, bool) {
	raw, _, err := store.monCommand(ctx, map[string]any{"prefix": "osd pool get", "pool": pool, "var": variable, "format": "json"})
	if err != nil {
		return 0, false
	}
	var payload map[string]any
	if json.Unmarshal(raw, &payload) != nil {
		return 0, false
	}
	value, ok := payload[variable].(float64)
	if !ok || value < 0 || math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, false
	}
	return uint64(value), true
}

func (store *radosObjectStore) monCommand(ctx context.Context, command map[string]any) ([]byte, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	raw, err := json.Marshal(command)
	if err != nil {
		return nil, "", err
	}
	result, commandErr := store.client.MonitorCommand(ctx, raw, nil)
	if commandErr != nil {
		return result.Output, result.Status, mapRadosGoError(commandErr)
	}
	return result.Output, result.Status, nil
}

func splitMonitors(value string) []string {
	return strings.FieldsFunc(value, func(character rune) bool {
		return character == ',' || character == ';' || character == ' ' || character == '\t' || character == '\n'
	})
}

func mapRadosGoError(err error) error {
	switch {
	case err == nil:
		return nil
	case stderrors.Is(err, radosgo.ErrNotFound):
		return fmt.Errorf("%w: %v", ErrNotFound, err)
	case stderrors.Is(err, radosgo.ErrExists):
		return fmt.Errorf("%w: %v", ErrExists, err)
	case stderrors.Is(err, radosgo.ErrPermission):
		return fmt.Errorf("%w: %v", syscall.EACCES, err)
	case stderrors.Is(err, radosgo.ErrInvalidArgument):
		return fmt.Errorf("%w: %v", syscall.EINVAL, err)
	default:
		return err
	}
}
