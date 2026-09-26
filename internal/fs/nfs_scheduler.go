package fs

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	client "github.com/willscott/go-nfs-client/nfs"
	"github.com/willscott/go-nfs-client/nfs/rpc"
)

type nfsServer struct {
	slots    chan struct{}
	reads    chan struct{}
	mutex    sync.Mutex
	mount    *rpc.Client
	fallback *nfsPool
}

type nfsOperation int

const (
	nfsLookup nfsOperation = iota
	nfsGetattr
	nfsReadDirPlus
	nfsRead
	nfsReadlink
)

var nfsOperationNames = [...]string{"lookup", "getattr", "readdirplus", "read", "readlink"}

func (filesystem *NFS) call(endpoint *nfsEndpoint, operation nfsOperation, call func(*client.Target) error) error {
	endpoint.lastUse.Store(time.Now().UnixNano())
	filesystem.maybeMaintainPools()
	pool, borrowed := endpoint.activePool()
	return pool.call(filesystem.ctx, operation == nfsRead, &filesystem.operations[operation], func(target *client.Target) error {
		if borrowed {
			var err error
			target, err = filesystem.bindTarget(endpoint, pool, target)
			if err != nil {
				return err
			}
		}
		return call(target)
	})
}

func newNFSServer(connections int) *nfsServer {
	return &nfsServer{slots: make(chan struct{}, connections), reads: make(chan struct{}, max(1, connections-1))}
}

type nfsPool struct {
	mutex     sync.Mutex
	size      atomic.Int64
	lastUse   atomic.Int64
	reserved  bool
	targets   []*client.Target
	available chan *client.Target
	metadata  chan *client.Target
	server    *nfsServer
}

func newNFSPool(targets []*client.Target, server *nfsServer) *nfsPool {
	pool := &nfsPool{available: make(chan *client.Target, cap(server.slots)), metadata: make(chan *client.Target, 1), server: server}
	for _, target := range targets {
		pool.add(target)
	}
	pool.lastUse.Store(time.Now().UnixNano())
	return pool
}

func (pool *nfsPool) add(target *client.Target) {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()
	pool.targets = append(pool.targets, target)
	if len(pool.targets) > 1 && !pool.reserved {
		pool.metadata <- target
		pool.reserved = true
	} else {
		pool.available <- target
	}
	pool.size.Store(int64(len(pool.targets)))
}

func (pool *nfsPool) snapshot() []*client.Target {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()
	return append([]*client.Target(nil), pool.targets...)
}

func (pool *nfsPool) trimIdle(now time.Time) int {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()
	if now.Sub(time.Unix(0, pool.lastUse.Load())) < 30*time.Second {
		return 0
	}
	var retired []*client.Target
	for len(pool.available) > 1 {
		select {
		case target := <-pool.available:
			retired = append(retired, target)
		default:
		}
	}
	if len(pool.available) > 0 {
		select {
		case target := <-pool.metadata:
			retired = append(retired, target)
			pool.reserved = false
		default:
		}
	}
	for _, target := range retired {
		for index, current := range pool.targets {
			if current == target {
				pool.targets = append(pool.targets[:index], pool.targets[index+1:]...)
				break
			}
		}
		target.Client.Close()
	}
	pool.size.Store(int64(len(pool.targets)))
	return len(retired)
}

func (pool *nfsPool) acquire(ctx context.Context, read bool) (*client.Target, func(), error) {
	pool.lastUse.Store(time.Now().UnixNano())
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if read {
		select {
		case pool.server.reads <- struct{}{}:
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}
	}
	releaseRead := func() {
		if read {
			<-pool.server.reads
		}
	}
	var target *client.Target
	reserved := false
	metadata := pool.metadata
	if read {
		metadata = nil
	} else {
		select {
		case target = <-metadata:
			reserved = true
		default:
		}
	}
	if target == nil {
		select {
		case target = <-metadata:
			reserved = true
		case target = <-pool.available:
		case <-ctx.Done():
			releaseRead()
			return nil, nil, ctx.Err()
		}
	}
	releaseTarget := func() {
		if reserved {
			pool.metadata <- target
		} else {
			pool.available <- target
		}
		releaseRead()
	}
	select {
	case pool.server.slots <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-pool.server.slots
			releaseTarget()
			return nil, nil, err
		}
		return target, func() { <-pool.server.slots; releaseTarget() }, nil
	case <-ctx.Done():
		releaseTarget()
		return nil, nil, ctx.Err()
	}
}

type NFSOperationStats struct {
	Attempts           uint64 `json:"attempts"`
	Calls              uint64 `json:"calls"`
	Errors             uint64 `json:"errors"`
	Cancellations      uint64 `json:"cancellations"`
	QueueNanoseconds   uint64 `json:"queue_nanoseconds"`
	ServiceNanoseconds uint64 `json:"service_nanoseconds"`
	Active             uint64 `json:"active"`
	MaxActive          uint64 `json:"max_active"`
}

type nfsOperationCounters struct {
	attempts, calls, errors, cancellations, queue, service, active, maximum atomic.Uint64
}

func (counters *nfsOperationCounters) stats() NFSOperationStats {
	return NFSOperationStats{Attempts: counters.attempts.Load(), Calls: counters.calls.Load(),
		Errors: counters.errors.Load(), Cancellations: counters.cancellations.Load(),
		QueueNanoseconds: counters.queue.Load(), ServiceNanoseconds: counters.service.Load(),
		Active: counters.active.Load(), MaxActive: counters.maximum.Load()}
}

func (pool *nfsPool) call(ctx context.Context, read bool, counters *nfsOperationCounters, call func(*client.Target) error) (err error) {
	defer func() {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			counters.cancellations.Add(1)
		}
	}()
	start := time.Now()
	counters.attempts.Add(1)
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	target, release, err := pool.acquire(waitCtx, read)
	cancel()
	counters.queue.Add(uint64(time.Since(start)))
	if err != nil {
		counters.errors.Add(1)
		return err
	}
	defer release()
	counters.calls.Add(1)
	active := counters.active.Add(1)
	for previous := counters.maximum.Load(); active > previous; previous = counters.maximum.Load() {
		if counters.maximum.CompareAndSwap(previous, active) {
			break
		}
	}
	start = time.Now()
	err = call(target)
	counters.service.Add(uint64(time.Since(start)))
	counters.active.Add(^uint64(0))
	if err != nil && !errors.Is(err, io.EOF) {
		counters.errors.Add(1)
	}
	return err
}
