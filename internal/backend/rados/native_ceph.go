//go:build rados

package rados

import (
	"bytes"
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"math"
	"strings"
	"time"

	cephrados "github.com/ceph/go-ceph/rados"
	"github.com/otuschhoff/vaultic/internal/backend"
)

type cephObjectStore struct {
	connection *cephrados.Conn
	ioctx      *cephrados.IOContext
	pool       string
	namespace  string
	cursors    nativeListCursorRegistry
}

func openNative(ctx context.Context, config Config) (driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	connection, err := cephrados.NewConnWithUser(strings.TrimPrefix(config.Client, "client."))
	if err != nil {
		return nil, mapNativeError(err)
	}
	failed := true
	defer func() {
		if failed {
			connection.Shutdown()
		}
	}()
	timeout := fmt.Sprintf("%d", max(1, int(config.OperationTTL.Seconds())))
	for option, value := range map[string]string{
		"mon_host": config.Monitors, "key": config.Key.Unwrap(),
		"rados_osd_op_timeout": timeout, "rados_mon_op_timeout": timeout,
		"client_mount_timeout": timeout,
	} {
		if err := connection.SetConfigOption(option, value); err != nil {
			return nil, fmt.Errorf("configure native RADOS %s: %w", option, mapNativeError(err))
		}
	}
	if err := connection.Connect(); err != nil {
		return nil, fmt.Errorf("connect native RADOS: %w", mapNativeError(err))
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	fsid, err := connection.GetFSID()
	if err != nil {
		return nil, fmt.Errorf("read native RADOS cluster identity: %w", mapNativeError(err))
	}
	if !strings.EqualFold(fsid, config.ClusterFSID) {
		return nil, fmt.Errorf("native RADOS cluster identity %q does not match sealed identity %q", fsid, config.ClusterFSID)
	}
	ioctx, err := connection.OpenIOContext(config.Pool)
	if err != nil {
		return nil, fmt.Errorf("open native RADOS pool %q: %w", config.Pool, mapNativeError(err))
	}
	ioctx.SetNamespace(config.Namespace)
	failed = false
	native := &nativeDriver{
		raw:                 &cephObjectStore{connection: connection, ioctx: ioctx, pool: config.Pool, namespace: config.Namespace},
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

func (store *cephObjectStore) stat(name string) (uint64, error) {
	value, err := store.ioctx.Stat(name)
	return value.Size, err
}

func (store *cephObjectStore) statWithModTime(name string) (uint64, time.Time, bool, error) {
	value, err := store.ioctx.Stat(name)
	if err != nil {
		return 0, time.Time{}, false, err
	}
	return value.Size, value.ModTime, true, nil
}

func (store *cephObjectStore) read(name string, buffer []byte, offset uint64) (int, error) {
	return store.ioctx.Read(name, buffer, offset)
}

func (store *cephObjectStore) write(name string, data []byte, exclusive bool) error {
	operation := cephrados.CreateWriteOp()
	defer operation.Release()
	if exclusive {
		operation.Create(cephrados.CreateExclusive)
	}
	operation.WriteFull(data)
	return operation.Operate(store.ioctx, name, cephrados.OperationNoFlag)
}

func (store *cephObjectStore) compareAndSwap(name string, expected []byte, replacement []byte, createOnly bool) (bool, error) {
	if createOnly {
		operation := cephrados.CreateWriteOp()
		defer operation.Release()
		operation.Create(cephrados.CreateExclusive)
		operation.WriteFull(replacement)
		err := operation.Operate(store.ioctx, name, cephrados.OperationNoFlag)
		if err == nil {
			return true, nil
		}
		if stderrors.Is(mapNativeError(err), ErrExists) {
			return false, nil
		}
		return false, err
	}
	ioctx, err := store.connection.OpenIOContext(store.pool)
	if err != nil {
		return false, err
	}
	defer ioctx.Destroy()
	ioctx.SetNamespace(store.namespace)
	stat, err := ioctx.Stat(name)
	if stderrors.Is(err, cephrados.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if stat.Size != uint64(len(expected)) {
		return false, nil
	}
	version, err := ioctx.GetLastVersion()
	if err != nil {
		return false, err
	}
	operation := cephrados.CreateWriteOp()
	defer operation.Release()
	operation.AssertVersion(version)
	cmpStep := operation.CmpExt(expected, 0)
	operation.WriteFull(replacement)
	err = operation.Operate(ioctx, name, cephrados.OperationNoFlag)
	if err == nil {
		return true, nil
	}
	if cmpStep.Result != 0 || stderrors.Is(mapNativeError(err), ErrNotFound) {
		return false, nil
	}
	currentStat, statErr := ioctx.Stat(name)
	if stderrors.Is(statErr, cephrados.ErrNotFound) || statErr == nil && currentStat.Size != uint64(len(expected)) {
		return false, nil
	}
	if statErr == nil {
		current := make([]byte, len(expected))
		read, readErr := ioctx.Read(name, current, 0)
		if readErr == nil && (read != len(current) || !bytes.Equal(current, expected)) {
			return false, nil
		}
	}
	return false, err
}

func (store *cephObjectStore) remove(name string) error {
	return store.ioctx.Delete(name)
}

func (store *cephObjectStore) listPage(ctx context.Context, prefix string, after string, limit int) ([]string, string, bool, error) {
	return store.cursors.page(ctx, prefix, after, limit, func() (nativeObjectIterator, error) {
		return store.ioctx.Iter()
	})
}

func (store *cephObjectStore) close() {
	store.cursors.close()
	store.ioctx.Destroy()
	store.connection.Shutdown()
}

func (store *cephObjectStore) capacity(ctx context.Context, pool string) (backend.CapacityTelemetrySample, error) {
	if err := ctx.Err(); err != nil {
		return backend.CapacityTelemetrySample{}, err
	}
	cluster, clusterErr := store.connection.GetClusterStats()
	if clusterErr != nil {
		return backend.CapacityTelemetrySample{}, mapNativeError(clusterErr)
	}
	poolStats, poolErr := store.ioctx.GetPoolStats()
	if poolErr != nil {
		return backend.CapacityTelemetrySample{}, mapNativeError(poolErr)
	}
	sample := backend.CapacityTelemetrySample{TotalRawBytes: uint64(cluster.Kb) * 1024, FreeRawBytes: uint64(cluster.Kb_avail) * 1024, Health: "healthy"}
	if sample.FreeRawBytes > sample.TotalRawBytes {
		sample.Inconsistent = true
	}
	if poolStats.Num_objects > 0 {
		amp := float64(poolStats.Num_object_copies) / float64(poolStats.Num_objects)
		if amp >= 1 && !math.IsInf(amp, 0) && !math.IsNaN(amp) {
			sample.RawAmplification = amp
		}
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

func (store *cephObjectStore) healthStatus(ctx context.Context) (string, bool) {
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

func (store *cephObjectStore) poolSpaceFacts(ctx context.Context, pool string) (uint64, uint64, uint64, bool) {
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

func (store *cephObjectStore) poolReplicaValue(ctx context.Context, pool string, variable string) (uint64, bool) {
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

func (store *cephObjectStore) monCommand(ctx context.Context, command map[string]any) ([]byte, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	raw, err := json.Marshal(command)
	if err != nil {
		return nil, "", err
	}
	response, status, commandErr := store.connection.MonCommand(raw)
	if commandErr != nil {
		return nil, status, mapNativeError(commandErr)
	}
	return response, status, nil
}
