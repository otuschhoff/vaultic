package fs

import (
	"errors"
	"time"

	client "github.com/willscott/go-nfs-client/nfs"
	"github.com/willscott/go-nfs-client/nfs/rpc"
)

func (endpoint *nfsEndpoint) activePool() (*nfsPool, bool) {
	if pool := endpoint.recovered.Load(); pool != nil {
		return pool, false
	}
	return endpoint.pool, endpoint.borrowed
}

func (filesystem *NFS) bindTarget(endpoint *nfsEndpoint, pool *nfsPool, original *client.Target) (*client.Target, error) {
	endpoint.bindingsMutex.Lock()
	defer endpoint.bindingsMutex.Unlock()
	if target := endpoint.bindings[original]; target != nil {
		return target, nil
	}
	current := make(map[*client.Target]bool)
	for _, target := range pool.snapshot() {
		current[target] = true
	}
	for target := range endpoint.bindings {
		if !current[target] {
			delete(endpoint.bindings, target)
		}
	}
	target, err := client.NewTargetWithClient(original.Client, filesystem.auth, endpoint.targets[0].RootHandle(), endpoint.root, 0)
	if err != nil {
		return nil, err
	}
	endpoint.bindings[original] = target
	return target, nil
}

func (filesystem *NFS) maybeMaintainPools() {
	now := time.Now()
	if now.UnixNano() < filesystem.nextMaintenance.Load() || !filesystem.maintaining.CompareAndSwap(false, true) {
		return
	}
	filesystem.mutex.Lock()
	if filesystem.ctx.Err() != nil {
		filesystem.maintaining.Store(false)
		filesystem.mutex.Unlock()
		return
	}
	filesystem.nextMaintenance.Store(now.Add(5 * time.Second).UnixNano())
	filesystem.maintenance.Add(1)
	filesystem.mutex.Unlock()
	go func() {
		defer filesystem.maintenance.Done()
		defer filesystem.maintaining.Store(false)
		filesystem.maintainPools(now)
	}()
}

func (filesystem *NFS) maintainPools(now time.Time) {
	filesystem.mutex.Lock()
	endpoints := make([]*nfsEndpoint, 0, len(filesystem.endpoints))
	for _, endpoint := range filesystem.endpoints {
		endpoints = append(endpoints, endpoint)
	}
	filesystem.mutex.Unlock()
	seen := make(map[*nfsPool]bool)
	var candidate *nfsEndpoint
	for _, endpoint := range endpoints {
		if filesystem.ctx.Err() != nil {
			return
		}
		endpoint.mutex.Lock()
		pool, borrowed := endpoint.activePool()
		ready := endpoint.initialized && endpoint.err == nil && pool != nil && endpoint.dial != nil
		endpoint.mutex.Unlock()
		if !ready {
			continue
		}
		if !seen[pool] {
			pool.server.mutex.Lock()
			filesystem.poolRetired.Add(uint64(pool.trimIdle(now)))
			pool.server.mutex.Unlock()
			seen[pool] = true
		}
		if now.Sub(time.Unix(0, endpoint.lastUse.Load())) >= 30*time.Second ||
			(!borrowed && pool.size.Load() >= int64(filesystem.options.Connections)) {
			continue
		}
		if candidate == nil || endpoint.lastGrowth.Load() < candidate.lastGrowth.Load() {
			candidate = endpoint
		}
	}
	if candidate != nil {
		candidate.lastGrowth.Store(now.UnixNano())
		filesystem.poolGrowthAttempts.Add(1)
		if err := filesystem.growPool(candidate); err != nil {
			if errors.Is(err, rpc.ErrNoReservedPort) {
				filesystem.poolShortfalls.Add(1)
			} else {
				filesystem.poolGrowthFailures.Add(1)
			}
		}
	}
}

func (filesystem *NFS) growPool(endpoint *nfsEndpoint) error {
	endpoint.server.mutex.Lock()
	defer endpoint.server.mutex.Unlock()
	pool, borrowed := endpoint.activePool()
	if borrowed {
		pool = nil
	}
	for pool == nil || pool.size.Load() < int64(filesystem.options.Connections) {
		if err := filesystem.ctx.Err(); err != nil {
			return err
		}
		connection, err := endpoint.dial(endpoint.source.Server, client.Nfs3Prog, filesystem.options.NFSPort)
		if err != nil {
			return err
		}
		filesystem.connectionsOpened.Add(1)
		target, err := client.NewTargetWithClient(connection, filesystem.auth, endpoint.targets[0].RootHandle(), endpoint.root, 0)
		if err == nil {
			err = filesystem.ctx.Err()
		}
		if err != nil {
			connection.Close()
			return err
		}
		if pool == nil {
			pool = newNFSPool([]*client.Target{target}, endpoint.server)
			endpoint.recovered.Store(pool)
		} else {
			pool.add(target)
		}
	}
	return nil
}
