//go:build rados || radosfake

package rados

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"
)

const (
	nativeListCursorLimit = 16
	nativeListCursorTTL   = 3 * time.Minute
)

var (
	errNativeListCursorExpired = errors.New("native RADOS list cursor expired")
	errNativeListCursorClosed  = errors.New("native RADOS list cursor registry closed")
)

type nativeObjectIterator interface {
	Next() bool
	Value() string
	Err() error
	Close()
}

type nativeListCursor struct {
	iterator nativeObjectIterator
	expires  time.Time
}

type nativeListCursorRegistry struct {
	mu      sync.Mutex
	cursors map[string]nativeListCursor
	active  sync.WaitGroup
	closed  bool
}

func (registry *nativeListCursorRegistry) page(
	ctx context.Context,
	prefix string,
	cursor string,
	limit int,
	create func() (nativeObjectIterator, error),
) ([]string, string, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", false, err
	}
	registry.mu.Lock()
	if registry.closed {
		registry.mu.Unlock()
		return nil, "", false, errNativeListCursorClosed
	}
	registry.active.Add(1)
	registry.mu.Unlock()
	defer registry.active.Done()
	iterator, err := registry.take(cursor, create)
	if err != nil {
		return nil, "", false, err
	}
	retained := false
	defer func() {
		if !retained {
			iterator.Close()
		}
	}()
	if limit <= 0 {
		limit = orphanChunkScanPageLimit
	}
	page := make([]string, 0, limit)
	for iterator.Next() {
		if err := ctx.Err(); err != nil {
			return nil, "", false, err
		}
		name := iterator.Value()
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		page = append(page, name)
		if len(page) == limit {
			next, ok := registry.put(iterator)
			if !ok {
				return nil, "", false, errNativeListCursorClosed
			}
			retained = true
			return page, next, false, nil
		}
	}
	if err := iterator.Err(); err != nil {
		return nil, "", false, err
	}
	return page, "", true, nil
}

func (registry *nativeListCursorRegistry) take(
	token string,
	create func() (nativeObjectIterator, error),
) (nativeObjectIterator, error) {
	if token == "" {
		return create()
	}
	now := time.Now()
	registry.mu.Lock()
	registry.expireLocked(now)
	if registry.closed {
		registry.mu.Unlock()
		return nil, errNativeListCursorClosed
	}
	cursor, ok := registry.cursors[token]
	if ok {
		delete(registry.cursors, token)
	}
	registry.mu.Unlock()
	if !ok {
		return nil, errNativeListCursorExpired
	}
	return cursor.iterator, nil
}

func (registry *nativeListCursorRegistry) put(iterator nativeObjectIterator) (string, bool) {
	now := time.Now()
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return "", false
	}
	registry.expireLocked(now)
	if registry.cursors == nil {
		registry.cursors = make(map[string]nativeListCursor)
	}
	for len(registry.cursors) >= nativeListCursorLimit {
		registry.evictOldestLocked()
	}
	for {
		token := readCacheFenceID()
		if _, exists := registry.cursors[token]; exists {
			continue
		}
		registry.cursors[token] = nativeListCursor{iterator: iterator, expires: now.Add(nativeListCursorTTL)}
		return token, true
	}
}

func (registry *nativeListCursorRegistry) expireLocked(now time.Time) {
	for token, cursor := range registry.cursors {
		if now.Before(cursor.expires) {
			continue
		}
		cursor.iterator.Close()
		delete(registry.cursors, token)
	}
}

func (registry *nativeListCursorRegistry) evictOldestLocked() {
	var oldestToken string
	var oldest nativeListCursor
	for token, cursor := range registry.cursors {
		if oldestToken == "" || cursor.expires.Before(oldest.expires) {
			oldestToken = token
			oldest = cursor
		}
	}
	if oldestToken != "" {
		oldest.iterator.Close()
		delete(registry.cursors, oldestToken)
	}
}

func (registry *nativeListCursorRegistry) close() {
	registry.mu.Lock()
	registry.closed = true
	for token, cursor := range registry.cursors {
		cursor.iterator.Close()
		delete(registry.cursors, token)
	}
	registry.mu.Unlock()
	registry.active.Wait()
}
