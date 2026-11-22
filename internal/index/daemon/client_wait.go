package daemon

import (
	"bytes"
	"context"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	vaulticdbv1 "github.com/otuschhoff/vaultic/internal/index/proto/vaulticdb/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type readTimingKey struct{}

// ReadTiming records validated timing headers for one synchronous daemon read.
type ReadTiming struct {
	ClientNS, HandlerNS, StorageNS uint64
	Valid                          bool
	DatabaseLockNS, EngineNS       uint64
	DatabaseTimingValid            bool
	BlobWork                       BlobReadWork
}

type BlobReadWork struct {
	SSTVisits, FilterNegatives, PlannedBlocks uint64
	Valid                                     bool
}

type BlobReadWorkTotals struct {
	Samples         uint64 `json:"samples,omitempty"`
	SSTVisits       uint64 `json:"sst_visits,omitempty"`
	FilterNegatives uint64 `json:"filter_negatives,omitempty"`
	PlannedBlocks   uint64 `json:"planned_blocks,omitempty"`
}

type blobReadWorkKey struct{}

func WithBlobReadWork(ctx context.Context, totals *BlobReadWorkTotals) context.Context {
	return context.WithValue(ctx, blobReadWorkKey{}, totals)
}

func blobReadWork(headers metadata.MD) BlobReadWork {
	var counts [3]uint64
	for index, name := range [...]string{"x-vaulticdb-blob-sst-visits", "x-vaulticdb-blob-filter-negatives", "x-vaulticdb-blob-planned-blocks"} {
		values := headers.Get(name)
		if len(values) != 1 {
			return BlobReadWork{}
		}
		count, err := strconv.ParseUint(values[0], 10, 64)
		if err != nil {
			return BlobReadWork{}
		}
		counts[index] = count
	}
	return BlobReadWork{SSTVisits: counts[0], FilterNegatives: counts[1], PlannedBlocks: counts[2], Valid: true}
}

func WithReadTiming(ctx context.Context, timing *ReadTiming) context.Context {
	return context.WithValue(ctx, readTimingKey{}, timing)
}

func databaseReadTiming(headers metadata.MD, storage uint64) (uint64, uint64, bool) {
	lockValues := headers.Get("x-vaulticdb-read-db-lock-ns")
	engineValues := headers.Get("x-vaulticdb-read-engine-ns")
	if len(lockValues) != 1 || len(engineValues) != 1 {
		return 0, 0, false
	}
	lock, lockErr := strconv.ParseUint(lockValues[0], 10, 64)
	engine, engineErr := strconv.ParseUint(engineValues[0], 10, 64)
	if lockErr != nil || engineErr != nil || lock > storage || engine > storage-lock {
		return 0, 0, false
	}
	return lock, engine, true
}

type RPCWaitStats struct {
	Method               string `json:"method"`
	Calls                uint64 `json:"calls"`
	Failures             uint64 `json:"failures"`
	ElapsedWorkerNS      uint64 `json:"elapsed_worker_ns"`
	Active               uint64 `json:"active"`
	OldestActiveNS       uint64 `json:"oldest_active_ns"`
	ServerTimingCalls    uint64 `json:"server_timing_calls"`
	ServerTimingClientNS uint64 `json:"server_timing_client_ns"`
	ServerHandlerNS      uint64 `json:"server_handler_ns"`
	ServerStorageNS      uint64 `json:"server_storage_ns"`
	OutsideHandlerNS     uint64 `json:"outside_handler_ns"`
	InvalidServerTimings uint64 `json:"invalid_server_timings"`
	BlobReadTimingCalls  uint64 `json:"blob_read_timing_calls"`
	BlobReadClientNS     uint64 `json:"blob_read_client_ns"`
	BlobReadHandlerNS    uint64 `json:"blob_read_handler_ns"`
	BlobReadStorageNS    uint64 `json:"blob_read_storage_ns"`
}

type rpcWaitCall struct {
	method  string
	started time.Time
}

type rpcWaitCounters struct {
	mu       sync.Mutex
	next     uint64
	methods  map[string]RPCWaitStats
	inflight map[uint64]rpcWaitCall
}

func (c *Client) RPCWaitStats() []RPCWaitStats {
	return c.rpcWait.snapshot()
}

func (counters *rpcWaitCounters) snapshot() []RPCWaitStats {
	counters.mu.Lock()
	defer counters.mu.Unlock()
	now := time.Now()
	values := make(map[string]RPCWaitStats, len(counters.methods))
	for method, value := range counters.methods {
		values[method] = value
	}
	for _, call := range counters.inflight {
		value := values[call.method]
		value.Method = call.method
		value.Active++
		value.OldestActiveNS = max(value.OldestActiveNS, uint64(now.Sub(call.started)))
		values[call.method] = value
	}
	result := make([]RPCWaitStats, 0, len(values))
	for _, value := range values {
		result = append(result, value)
	}
	sort.Slice(result, func(first, second int) bool { return result[first].Method < result[second].Method })
	return result
}

func (counters *rpcWaitCounters) intercept(ctx context.Context, method string, request, response any, connection *grpc.ClientConn, invoke grpc.UnaryInvoker, options ...grpc.CallOption) error {
	name := method[strings.LastIndex(method, "/")+1:]
	known := false
	for _, descriptor := range vaulticdbv1.VaulticDB_ServiceDesc.Methods {
		if descriptor.MethodName == name {
			known = true
			break
		}
	}
	if !known {
		return invoke(ctx, method, request, response, connection, options...)
	}
	started := time.Now()
	counters.mu.Lock()
	if counters.methods == nil {
		counters.methods = make(map[string]RPCWaitStats)
		counters.inflight = make(map[uint64]rpcWaitCall)
	}
	counters.next++
	id := counters.next
	counters.inflight[id] = rpcWaitCall{method: name, started: started}
	counters.mu.Unlock()
	var headers metadata.MD
	if name == "Get" || name == "MultiGet" {
		options = append(options, grpc.Header(&headers))
	}
	err := invoke(ctx, method, request, response, connection, options...)
	elapsed := uint64(time.Since(started))
	counters.mu.Lock()
	delete(counters.inflight, id)
	value := counters.methods[name]
	value.Method = name
	value.Calls++
	value.ElapsedWorkerNS += elapsed
	if err != nil {
		value.Failures++
	} else {
		handlerValues := headers.Get("x-vaulticdb-read-handler-ns")
		storageValues := headers.Get("x-vaulticdb-read-storage-ns")
		if len(handlerValues) != 0 || len(storageValues) != 0 {
			valid := len(handlerValues) == 1 && len(storageValues) == 1
			var handler, storage uint64
			if valid {
				var handlerError, storageError error
				handler, handlerError = strconv.ParseUint(handlerValues[0], 10, 64)
				storage, storageError = strconv.ParseUint(storageValues[0], 10, 64)
				valid = handlerError == nil && storageError == nil && storage <= handler && handler <= elapsed
			}
			if valid {
				if timing, ok := ctx.Value(readTimingKey{}).(*ReadTiming); ok && timing != nil {
					*timing = ReadTiming{ClientNS: elapsed, HandlerNS: handler, StorageNS: storage, Valid: true}
					timing.DatabaseLockNS, timing.EngineNS, timing.DatabaseTimingValid = databaseReadTiming(headers, storage)
					timing.BlobWork = blobReadWork(headers)
				}
				if totals, ok := ctx.Value(blobReadWorkKey{}).(*BlobReadWorkTotals); ok && totals != nil {
					if work := blobReadWork(headers); work.Valid {
						totals.Samples++
						totals.SSTVisits += work.SSTVisits
						totals.FilterNegatives += work.FilterNegatives
						totals.PlannedBlocks += work.PlannedBlocks
					}
				}
				value.ServerTimingCalls++
				value.ServerTimingClientNS += elapsed
				value.ServerHandlerNS += handler
				value.ServerStorageNS += storage
				value.OutsideHandlerNS += elapsed - handler
				var keys [][]byte
				switch read := request.(type) {
				case *vaulticdbv1.GetRequest:
					keys = [][]byte{read.GetKey()}
				case *vaulticdbv1.MultiGetRequest:
					keys = read.GetKeys()
				}
				blobs := len(keys) != 0
				for _, key := range keys {
					if !bytes.HasPrefix(key, []byte("b:")) {
						blobs = false
						break
					}
				}
				if blobs {
					value.BlobReadTimingCalls++
					value.BlobReadClientNS += elapsed
					value.BlobReadHandlerNS += handler
					value.BlobReadStorageNS += storage
				}
			} else {
				value.InvalidServerTimings++
			}
		}
	}
	counters.methods[name] = value
	counters.mu.Unlock()
	return err
}
