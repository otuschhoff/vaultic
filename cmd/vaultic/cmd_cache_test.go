package main

import (
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/spf13/pflag"
)

func cacheUpdateFlagSet() *pflag.FlagSet {
	flags := pflag.NewFlagSet("cache-update", pflag.ContinueOnError)
	flags.String("trust", "", "")
	flags.String("trust-ack", "", "")
	flags.Uint64("max-bytes", 0, "")
	flags.Uint64("chunk-bytes", 0, "")
	flags.Duration("idle-age", 0, "")
	flags.Duration("absolute-age", 0, "")
	flags.Uint32("read-priority", 0, "")
	flags.Uint32("admission-priority", 0, "")
	return flags
}

func TestValidateCacheUpdateOptions(t *testing.T) {
	t.Run("reject negative idle age", func(t *testing.T) {
		flags := cacheUpdateFlagSet()
		if err := flags.Set("idle-age", "-1s"); err != nil {
			t.Fatal(err)
		}
		if err := validateCacheUpdateOptions(flags, cacheUpdateOptions{IdleAge: -time.Second}); err == nil {
			t.Fatal("expected idle-age validation error")
		}
	})

	t.Run("reject absolute age shorter than idle age", func(t *testing.T) {
		flags := cacheUpdateFlagSet()
		if err := flags.Set("idle-age", "2h"); err != nil {
			t.Fatal(err)
		}
		if err := flags.Set("absolute-age", "1h"); err != nil {
			t.Fatal(err)
		}
		if err := validateCacheUpdateOptions(flags, cacheUpdateOptions{IdleAge: 2 * time.Hour, AbsoluteAge: time.Hour}); err == nil {
			t.Fatal("expected absolute-age validation error")
		}
	})

	t.Run("allow zero max-bytes for drain", func(t *testing.T) {
		flags := cacheUpdateFlagSet()
		if err := flags.Set("max-bytes", "0"); err != nil {
			t.Fatal(err)
		}
		err := validateCacheUpdateOptions(flags, cacheUpdateOptions{MaxBytes: 0})
		if err != nil {
			t.Fatalf("unexpected max-bytes validation error: %v", err)
		}
	})

	t.Run("reject zero chunk-bytes", func(t *testing.T) {
		flags := cacheUpdateFlagSet()
		if err := flags.Set("chunk-bytes", "0"); err != nil {
			t.Fatal(err)
		}
		err := validateCacheUpdateOptions(flags, cacheUpdateOptions{ChunkBytes: 0})
		if err == nil {
			t.Fatal("expected chunk-bytes validation error")
		}
	})

	for name, chunkBytes := range map[string]uint64{
		"above practical maximum": repository.MaxReadCacheChunkBytes + 1,
		"unsigned overflow":       math.MaxUint64,
	} {
		t.Run("reject "+name, func(t *testing.T) {
			flags := cacheUpdateFlagSet()
			if err := flags.Set("chunk-bytes", strconv.FormatUint(chunkBytes, 10)); err != nil {
				t.Fatal(err)
			}
			if err := validateCacheUpdateOptions(flags, cacheUpdateOptions{ChunkBytes: chunkBytes}); err == nil {
				t.Fatalf("expected chunk-bytes %d validation error", chunkBytes)
			}
		})
	}

	t.Run("accept practical maximum chunk-bytes", func(t *testing.T) {
		flags := cacheUpdateFlagSet()
		if err := flags.Set("chunk-bytes", strconv.FormatUint(repository.MaxReadCacheChunkBytes, 10)); err != nil {
			t.Fatal(err)
		}
		if err := validateCacheUpdateOptions(flags, cacheUpdateOptions{ChunkBytes: repository.MaxReadCacheChunkBytes}); err != nil {
			t.Fatalf("expected maximum chunk-bytes to be accepted: %v", err)
		}
	})

	t.Run("reject invalid trust mode", func(t *testing.T) {
		flags := cacheUpdateFlagSet()
		if err := flags.Set("trust", "invalid"); err != nil {
			t.Fatal(err)
		}
		err := validateCacheUpdateOptions(flags, cacheUpdateOptions{Trust: "invalid"})
		if err == nil {
			t.Fatal("expected trust validation error")
		}
	})

	t.Run("reject plaintext trust without ack", func(t *testing.T) {
		flags := cacheUpdateFlagSet()
		if err := flags.Set("trust", "plaintext-allowed"); err != nil {
			t.Fatal(err)
		}
		err := validateCacheUpdateOptions(flags, cacheUpdateOptions{Trust: "plaintext-allowed"})
		if err == nil {
			t.Fatal("expected plaintext trust ack validation error")
		}
	})

	t.Run("accept plaintext trust with ack", func(t *testing.T) {
		flags := cacheUpdateFlagSet()
		if err := flags.Set("trust", "plaintext-allowed"); err != nil {
			t.Fatal(err)
		}
		if err := flags.Set("trust-ack", "true"); err != nil {
			t.Fatal(err)
		}
		err := validateCacheUpdateOptions(flags, cacheUpdateOptions{Trust: "plaintext-allowed", TrustAck: "true"})
		if err != nil {
			t.Fatalf("expected validation success, got %v", err)
		}
	})
}
