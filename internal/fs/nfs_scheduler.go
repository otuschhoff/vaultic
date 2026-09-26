package fs

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"time"

	client "github.com/willscott/go-nfs-client/nfs"
)

type nfsServer struct {
	slots chan struct{}
	reads chan struct{}
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
	return endpoint.pool.call(filesystem.ctx, operation == nfsRead, &filesystem.operations[operation], call)
}

func newNFSServer(connections int) *nfsServer {
	return &nfsServer{slots: make(chan struct{}, connections), reads: make(chan struct{}, max(1, connections-1))}
}

type nfsPool struct {
	available chan *client.Target
	metadata  chan *client.Target
	server    *nfsServer
}

func newNFSPool(targets []*client.Target, server *nfsServer) *nfsPool {
	pool := &nfsPool{available: make(chan *client.Target, len(targets)), metadata: make(chan *client.Target, 1), server: server}
	for index, target := range targets {
		if index == 0 && len(targets) > 1 {
			pool.metadata <- target
		} else {
			pool.available <- target
		}
	}
	return pool
}

func (pool *nfsPool) acquire(ctx context.Context, read bool) (*client.Target, func(), error) {
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
