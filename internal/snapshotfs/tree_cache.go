package snapshotfs

import (
	"container/list"
	"context"
	"sync"

	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

type cachedTree struct {
	id    vaultic.ID
	nodes []*data.Node
	size  int
}

type treeCache struct {
	mu         sync.Mutex
	limit      int
	used       int
	entries    map[vaultic.ID]*list.Element
	lru        list.List
	inProgress map[vaultic.ID]chan struct{}
}

func newTreeCache(limit int) *treeCache {
	return &treeCache{
		limit:      limit,
		entries:    make(map[vaultic.ID]*list.Element),
		inProgress: make(map[vaultic.ID]chan struct{}),
	}
}

func (cache *treeCache) getOrCompute(ctx context.Context, id vaultic.ID, compute func() ([]*data.Node, int, error)) ([]*data.Node, error) {
	for {
		cache.mu.Lock()
		if element := cache.entries[id]; element != nil {
			cache.lru.MoveToFront(element)
			nodes := element.Value.(*cachedTree).nodes
			cache.mu.Unlock()
			return nodes, nil
		}
		if wait := cache.inProgress[id]; wait != nil {
			cache.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, canceledError(ctx.Err())
			case <-wait:
				continue
			}
		}

		finished := make(chan struct{})
		cache.inProgress[id] = finished
		cache.mu.Unlock()

		nodes, size, err := compute()

		cache.mu.Lock()
		delete(cache.inProgress, id)
		if err == nil && size <= cache.limit {
			for cache.used+size > cache.limit {
				cache.removeOldest()
			}
			entry := &cachedTree{id: id, nodes: nodes, size: size}
			cache.entries[id] = cache.lru.PushFront(entry)
			cache.used += size
		}
		close(finished)
		cache.mu.Unlock()
		return nodes, err
	}
}

func (cache *treeCache) removeOldest() {
	element := cache.lru.Back()
	if element == nil {
		return
	}
	entry := element.Value.(*cachedTree)
	delete(cache.entries, entry.id)
	cache.lru.Remove(element)
	cache.used -= entry.size
}

func (cache *treeCache) clear() {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	for _, element := range cache.entries {
		entry := element.Value.(*cachedTree)
		for index := range entry.nodes {
			entry.nodes[index] = nil
		}
	}
	cache.entries = make(map[vaultic.ID]*list.Element)
	cache.lru.Init()
	cache.used = 0
}

func (cache *treeCache) usage() (used, limit int) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	return cache.used, cache.limit
}
