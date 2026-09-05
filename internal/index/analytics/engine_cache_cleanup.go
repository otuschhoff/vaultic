package analytics

import (
	"context"
	"encoding/json"
	"sort"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
)

type expiringCacheEntry struct {
	key     []byte
	expires int64
	rank    int64
}

type heatCacheEntry struct {
	key     []byte
	updated int64
}

func cleanupCache(ctx context.Context, store Store, maximum int) error {
	entries, err := collectExpiringCacheEntries(ctx, store, "aq:result:")
	if err != nil {
		return err
	}
	if len(entries) > maximum {
		sort.Slice(entries, func(i, j int) bool { return entries[i].expires < entries[j].expires })
	} else {
		entries = nil
	}
	deleteCount := len(entries) - maximum
	if deleteCount < 0 {
		deleteCount = 0
	}
	deletes := make([][]byte, deleteCount)
	for index := range deletes {
		deletes[index] = entries[index].key
	}

	heatEntries, err := collectHeatCacheEntries(ctx, store)
	if err != nil {
		return err
	}
	if len(heatEntries) > maximum {
		sort.Slice(heatEntries, func(i, j int) bool { return heatEntries[i].updated < heatEntries[j].updated })
		for _, entry := range heatEntries[:len(heatEntries)-maximum] {
			deletes = append(deletes, entry.key)
		}
	}

	viewEntries, err := collectExpiringCacheEntries(ctx, store, "aq:view:")
	if err != nil {
		return err
	}
	sort.Slice(viewEntries, func(i, j int) bool {
		if viewEntries[i].rank != viewEntries[j].rank {
			return viewEntries[i].rank < viewEntries[j].rank
		}
		return viewEntries[i].expires < viewEntries[j].expires
	})
	for len(viewEntries) > maximum {
		deletes = append(deletes, viewEntries[0].key)
		viewEntries = viewEntries[1:]
	}
	for len(viewEntries) > 0 && viewEntries[0].expires < time.Now().Unix() {
		deletes = append(deletes, viewEntries[0].key)
		viewEntries = viewEntries[1:]
	}
	if len(deletes) == 0 {
		return nil
	}
	return store.WriteMutableBatch(ctx, nil, deletes, false)
}

func collectExpiringCacheEntries(ctx context.Context, store Store, prefix string) ([]expiringCacheEntry, error) {
	var entries []expiringCacheEntry
	err := scan(ctx, store, []byte(prefix), func(kv daemon.KeyValue) error {
		expires := int64(0)
		rank := int64(0)
		if record, err := schema.UnmarshalAnalyticsQueryRecord(kv.Value); err == nil {
			expires, rank = decodeExpiringCachePayload(record.Payload, prefix == "aq:view:")
		}
		entries = append(entries, expiringCacheEntry{key: append([]byte(nil), kv.Key...), expires: expires, rank: rank})
		return nil
	})
	return entries, err
}

func decodeExpiringCachePayload(payload []byte, viewEntry bool) (expires, rank int64) {
	if viewEntry {
		var view viewRecord
		if json.Unmarshal(payload, &view) == nil {
			return view.ExpiresAt, view.LastUsed
		}
		return 0, 0
	}
	var cached cacheRecord
	if json.Unmarshal(payload, &cached) == nil {
		return cached.ExpiresAt, 0
	}
	return 0, 0
}

func collectHeatCacheEntries(ctx context.Context, store Store) ([]heatCacheEntry, error) {
	var entries []heatCacheEntry
	err := scan(ctx, store, []byte("aq:heat:"), func(kv daemon.KeyValue) error {
		updated := int64(0)
		if record, err := schema.UnmarshalAnalyticsQueryRecord(kv.Value); err == nil {
			var heat heatRecord
			if json.Unmarshal(record.Payload, &heat) == nil {
				updated = heat.UpdatedAt
			}
		}
		entries = append(entries, heatCacheEntry{key: append([]byte(nil), kv.Key...), updated: updated})
		return nil
	})
	return entries, err
}
