package daemon

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	vaulticdbv1 "github.com/otuschhoff/vaultic/internal/index/proto/vaulticdb/v1"
	"google.golang.org/grpc"
)

type RPCWaitStats struct {
	Method          string `json:"method"`
	Calls           uint64 `json:"calls"`
	Failures        uint64 `json:"failures"`
	ElapsedWorkerNS uint64 `json:"elapsed_worker_ns"`
	Active          uint64 `json:"active"`
	OldestActiveNS  uint64 `json:"oldest_active_ns"`
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
	}
	counters.methods[name] = value
	counters.mu.Unlock()
	return err
}
