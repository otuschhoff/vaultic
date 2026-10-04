package workingkv

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/otuschhoff/vaultic/internal/workingbench"
)

func TestM5WorkingPolicyMeasurementBoundary(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		policy, err := NewPolicy(ModeRAM, 32<<20, "")
		if err != nil {
			t.Fatal(err)
		}
		var calls atomic.Uint64
		var unavailable atomic.Bool
		stop := policy.Start(t.Context(), func() (Resources, error) {
			calls.Add(1)
			if unavailable.Load() {
				return Resources{}, ErrInvalid
			}
			return Resources{HostLimit: 1 << 30, HostAvailable: 1 << 30, RSSKnown: true}, nil
		}, func(PolicyEvent) {})
		defer stop()
		time.Sleep(2 * time.Second)
		synctest.Wait()
		if calls.Load() == 0 {
			t.Fatal("live measurement policy never sampled resources")
		}
		stop()
		endpoint, samples := policy.Budget(), calls.Load()
		unavailable.Store(true)
		time.Sleep(2 * time.Second)
		synctest.Wait()
		if policy.Budget() != endpoint || calls.Load() != samples {
			t.Fatal("untimed verification mutated the measured policy endpoint")
		}
	})
}

func TestM5WorkingSet(t *testing.T) {
	root := os.Getenv("VAULTIC_PHASE35_M5_ROOT")
	if root == "" {
		t.Skip("isolated M5 runner not selected")
	}
	marker, err := os.ReadFile(filepath.Join(root, ".nobackup"))
	if err != nil || string(marker) != "Disposable Phase 35 M5 benchmark fixture.\n" {
		t.Fatal("not an owned M5 fixture", err)
	}
	mode := os.Getenv("VAULTIC_PHASE35_M5_MODE")
	limit := os.Getenv("VAULTIC_PHASE35_M5_LIMIT")
	target, err := strconv.ParseUint(os.Getenv("VAULTIC_PHASE35_M5_PAYLOAD"), 10, 64)
	if err != nil || target == 0 || target > 100_000_000_000 {
		t.Fatal("invalid frozen payload", err)
	}
	output := os.Getenv("VAULTIC_PHASE35_M5_OUTPUT")
	scratch := t.TempDir()
	if err := os.Chmod(scratch, 0700); err != nil {
		t.Fatal(err)
	}
	resources, err := ReadResources()
	if err != nil {
		t.Fatal(err)
	}
	policyMode := mode
	if policyMode == "pebble" {
		policyMode = "ram"
	}
	diskBudget := "16GiB"
	payloadSize, batchCount := 4096, 128
	if target > 1_000_000_000 {
		diskBudget = "512GiB"
		payloadSize, batchCount = 32768, 31
	}
	policy, err := Resolve(Configuration{Mode: policyMode, RAMLimit: limit, Scratch: scratch,
		DiskBudget: diskBudget, Reserve: "256MiB"}, resources)
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	var stop func()
	if mode != "pebble" {
		ctx = WithPolicy(ctx, policy)
		stop = policy.Start(ctx, ReadResources, func(PolicyEvent) {})
		defer stop()
	}
	store, err := OpenWorking(ctx, scratch, "m5-store-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	key := sha256.Sum256([]byte("synthetic M5 fixture encryption key"))
	block, _ := aes.NewCipher(key[:])
	sealed, _ := cipher.NewGCM(block)
	store.SetVerifier(func(key, value []byte) error {
		if len(value) < sealed.NonceSize()+sealed.Overhead() {
			return ErrWorkingStateCorrupt
		}
		_, err := sealed.Open(nil, value[:sealed.NonceSize()], value[sealed.NonceSize():], key)
		return err
	})
	plain := make([]byte, payloadSize)
	for index := range plain {
		plain[index] = byte(index*37 + 11)
	}
	keys := make([]byte, batchCount*8)
	values := make([]byte, batchCount*(payloadSize+sealed.NonceSize()+sealed.Overhead()))
	entries := make([]Entry, batchCount)
	encode := func(ordinal uint64, key, output []byte) []byte {
		binary.BigEndian.PutUint64(key, ordinal)
		nonce := output[:sealed.NonceSize()]
		clear(nonce)
		binary.BigEndian.PutUint64(nonce[4:], ordinal+1)
		return sealed.Seal(nonce, nonce, plain, key)
	}
	inputHash := sha256.New()
	for ordinal := uint64(0); ordinal < (target+uint64(payloadSize)-1)/uint64(payloadSize); ordinal++ {
		value := encode(ordinal, keys[:8], values[:payloadSize+sealed.NonceSize()+sealed.Overhead()])
		_, _ = inputHash.Write(keys[:8])
		_, _ = inputHash.Write(value)
	}
	inputDigest := hex.EncodeToString(inputHash.Sum(nil))
	probe, err := workingbench.StartWorkingProbe(scratch, output)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = probe.Finish("incomplete", nil) })
	if target > 1_000_000_000 {
		if err := probe.EnableLatencyHistogram(); err != nil {
			t.Fatal(err)
		}
	}
	var committed uint64
	var failure error
	var migrationSeconds float64
	count := (target + uint64(payloadSize) - 1) / uint64(payloadSize)
	for first := uint64(0); first < count; first += uint64(batchCount) {
		length := min(uint64(batchCount), count-first)
		for ordinal := uint64(0); ordinal < length; ordinal++ {
			key := keys[ordinal*8 : (ordinal+1)*8]
			valueSize := payloadSize + sealed.NonceSize() + sealed.Overhead()
			value := values[int(ordinal)*valueSize : (int(ordinal)+1)*valueSize]
			entries[ordinal] = Entry{key, encode(first+ordinal, key, value)}
		}
		beforeBackend := store.Backend()
		started := time.Now()
		err := store.Put(ctx, entries[:length])
		duration := time.Since(started)
		if latencyErr := probe.Latency(duration); latencyErr != nil {
			t.Fatal(latencyErr)
		}
		if beforeBackend == "ram" && store.Backend() == "bbolt" {
			migrationSeconds = duration.Seconds()
		}
		if err != nil {
			if !errors.Is(err, ErrWorkingMemoryLimitExceeded) {
				t.Fatal("nonadmission failure", err)
			}
			failure = err
			break
		}
		committed += length
	}
	var concurrentBytes atomic.Uint64
	if strings.HasPrefix(os.Getenv("VAULTIC_PHASE35_M5_SCENARIO"), "high-live") && failure == nil {
		finished := make(chan struct{})
		allocated := make(chan struct{})
		go func() {
			defer close(finished)
			defer close(allocated)
			retained := make([][]byte, 16)
			for ordinal := range 768 {
				buffer := make([]byte, 4<<20)
				for index := 0; index < len(buffer); index += 4096 {
					buffer[index] = byte(ordinal)
				}
				retained[ordinal%len(retained)] = buffer
				concurrentBytes.Add(uint64(len(buffer)))
				allocated <- struct{}{}
			}
			runtime.KeepAlive(retained)
		}()
		var lookupOrdinal uint64
		for range allocated {
			for range 2 {
				binary.BigEndian.PutUint64(keys[:8], (lookupOrdinal*7919)%committed)
				lookupOrdinal++
				started := time.Now()
				_, found, err := store.Get(ctx, keys[:8])
				if err != nil || !found {
					t.Fatal("concurrent required-state lookup failed", err)
				}
				if err := probe.LookupLatency(time.Since(started)); err != nil {
					t.Fatal(err)
				}
			}
		}
		<-finished
	}
	if stop != nil {
		stop()
	}
	state := policy.Snapshot("benchmark", "endpoint")
	if mode != "pebble" && state.Memory.Used > state.Memory.Limit {
		t.Fatal("aggregate overspend")
	}
	actualBackend := store.Backend()
	_, err = probe.Finish(inputDigest, map[string]any{"policy": state, "backend": actualBackend,
		"requested_mode": mode, "target_payload_bytes": target, "achieved_payload_bytes": committed * uint64(payloadSize),
		"payload_size": payloadSize, "batch_entries": batchCount,
		"committed_entries": committed, "completed": failure == nil, "typed_exhaustion": failure != nil,
		"migration_operation_seconds": migrationSeconds, "concurrent_allocated_bytes": concurrentBytes.Load()})
	if err != nil {
		t.Fatal(err)
	}
	var after []byte
	var scanned uint64
	resultHash := sha256.New()
	scanCount := 1
	if target > 1_000_000_000 {
		scanCount = 128
	}
	for {
		rows, err := store.Scan(context.Background(), nil, after, scanCount)
		if errors.Is(err, ErrWorkingMemoryLimitExceeded) && scanCount > 1 {
			scanCount = max(1, scanCount/2)
			continue
		}
		if err != nil {
			t.Fatal("valid retained state unreadable after admission failure", err)
		}
		if len(rows) == 0 {
			break
		}
		for _, row := range rows {
			if len(row.Key) != 8 || binary.BigEndian.Uint64(row.Key) != scanned {
				t.Fatal("duplicate, unordered or missing retained entry")
			}
			expected := encode(scanned, keys[:8], values[:payloadSize+sealed.NonceSize()+sealed.Overhead()])
			if !bytes.Equal(row.Value, expected) {
				t.Fatal("corrupt retained ciphertext")
			}
			_, _ = resultHash.Write(row.Key)
			_, _ = resultHash.Write(row.Value)
			after = row.Key
			scanned++
		}
	}
	if scanned != committed || (failure == nil && hex.EncodeToString(resultHash.Sum(nil)) != inputDigest) {
		t.Fatal("retained sequence digest mismatch")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if mode != "pebble" && (policy.Budget().Used != 0 || policy.Snapshot("closed", "endpoint").Disk.Used != 0) {
		t.Fatal("released store leaked reservations")
	}
	files, err := os.ReadDir(scratch)
	if err != nil || len(files) != 0 || (mode == "ram" && actualBackend != "ram") {
		t.Fatal("unsafe backend or scratch cleanup", err)
	}
	semantic := map[string]any{"complete": true, "input_sha256": inputDigest,
		"retained_sha256": hex.EncodeToString(resultHash.Sum(nil)), "sequence_verified": true,
		"verification_scan_rows": scanCount,
		"completed_workload":     failure == nil, "typed_exhaustion": failure != nil, "scratch_clean": true}
	encoded, _ := json.MarshalIndent(semantic, "", "  ")
	if err := os.WriteFile(filepath.Join(output, "semantic.json"), append(encoded, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}
