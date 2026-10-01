package workingkv

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/telemetry"
)

type replayFixture struct {
	rounds [][]Entry
	final  []Entry
	plain  map[string][]byte
	aead   cipher.AEAD
	kind   string
}

func fixtureToken(value string) []byte {
	mac := hmac.New(sha256.New, []byte("synthetic-phase35-m1-token-key"))
	_, _ = mac.Write([]byte(value))
	return mac.Sum(nil)
}

func newReplayFixture(kind string) replayFixture {
	key := sha256.Sum256([]byte("synthetic-phase35-m1-" + kind))
	block, _ := aes.NewCipher(key[:])
	aead, _ := cipher.NewGCM(block)
	fixture := replayFixture{kind: kind, aead: aead, plain: make(map[string][]byte)}
	count, rounds := 8192, 1
	if kind == "markers" {
		rounds = 4
	}
	if kind == "directories" {
		count = 529
	}
	for revision := range rounds {
		entries := make([]Entry, 0, count)
		for ordinal := range count {
			token := fixtureToken(fmt.Sprintf("%s/%d", kind, ordinal))
			var plain []byte
			switch kind {
			case "overlay":
				token = append(fixtureToken(fmt.Sprintf("blob/%d", ordinal/2)), fixtureToken(fmt.Sprintf("location/%d", ordinal))...)
				plain = make([]byte, 56)
				copy(plain, fixtureToken(fmt.Sprintf("pack/%d", ordinal)))
				binary.LittleEndian.PutUint64(plain[32:40], uint64(ordinal*128))
				binary.LittleEndian.PutUint64(plain[40:48], 128)
				binary.LittleEndian.PutUint64(plain[48:56], 128)
			case "markers":
				plain = []byte{byte((ordinal + revision) % 2)}
			case "directories":
				plain = []byte("[]")
				if ordinal == 0 {
					names := make([]string, 512)
					for index := range names {
						names[index] = fmt.Sprintf("child-%04d", index)
					}
					plain, _ = json.Marshal(names)
				} else if ordinal >= 513 {
					sizes := []int{4 << 10, 64 << 10, 512 << 10}
					plain, _ = json.Marshal([]string{strings.Repeat("x", sizes[(ordinal-513)%len(sizes)])})
				}
			}
			nonce := make([]byte, aead.NonceSize())
			binary.BigEndian.PutUint64(nonce[4:], uint64(revision*count+ordinal+1))
			aad := token
			if kind != "overlay" {
				aad = append([]byte("v1/"+kind+"/"), token...)
			}
			value := aead.Seal(nonce, nonce, plain, aad)
			entries = append(entries, Entry{token, value})
			fixture.plain[string(token)] = plain
		}
		fixture.rounds = append(fixture.rounds, entries)
	}
	fixture.final = append([]Entry(nil), fixture.rounds[len(fixture.rounds)-1]...)
	sort.Slice(fixture.final, func(left, right int) bool {
		return bytes.Compare(fixture.final[left].Key, fixture.final[right].Key) < 0
	})
	return fixture
}

func entriesDigest(entries []Entry) string {
	hash := sha256.New()
	var length [8]byte
	for _, entry := range entries {
		for _, value := range [][]byte{entry.Key, entry.Value} {
			binary.BigEndian.PutUint64(length[:], uint64(len(value)))
			_, _ = hash.Write(length[:])
			_, _ = hash.Write(value)
		}
	}
	return hex.EncodeToString(hash.Sum(nil))
}

type replayResources struct {
	RSSBytes         uint64   `json:"rss_bytes"`
	MappedStoreBytes uint64   `json:"mapped_store_bytes"`
	ScratchBytes     uint64   `json:"scratch_bytes"`
	ReadBytes        uint64   `json:"read_bytes"`
	WriteBytes       uint64   `json:"write_bytes"`
	ReadCalls        uint64   `json:"read_calls"`
	WriteCalls       uint64   `json:"write_calls"`
	CPUTicks         uint64   `json:"cpu_ticks"`
	Unavailable      []string `json:"unavailable,omitempty"`
}

func readReplayResources(root string) replayResources {
	var result replayResources
	for _, source := range []string{"status", "io", "stat", "maps"} {
		data, err := os.ReadFile("/proc/self/" + source)
		if err != nil {
			result.Unavailable = append(result.Unavailable, source)
			continue
		}
		for _, line := range strings.Split(string(data), "\n") {
			fields := strings.Fields(line)
			if len(fields) < 2 {
				continue
			}
			value, _ := strconv.ParseUint(fields[1], 10, 64)
			switch fields[0] {
			case "VmRSS:":
				result.RSSBytes = value * 1024
			case "read_bytes:":
				result.ReadBytes = value
			case "write_bytes:":
				result.WriteBytes = value
			case "syscr:":
				result.ReadCalls = value
			case "syscw:":
				result.WriteCalls = value
			}
			if source == "maps" && strings.Contains(line, root+"/") {
				bounds := strings.Split(fields[0], "-")
				if len(bounds) == 2 {
					lower, _ := strconv.ParseUint(bounds[0], 16, 64)
					upper, _ := strconv.ParseUint(bounds[1], 16, 64)
					result.MappedStoreBytes += upper - lower
				}
			}
		}
		if source == "stat" {
			fields := strings.Fields(string(data)[strings.LastIndex(string(data), ")")+1:])
			if len(fields) > 12 {
				user, _ := strconv.ParseUint(fields[11], 10, 64)
				system, _ := strconv.ParseUint(fields[12], 10, 64)
				result.CPUTicks = user + system
			}
		}
	}
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			info, err := entry.Info()
			if err != nil {
				return err
			}
			result.ScratchBytes += uint64(info.Size())
		}
		return nil
	})
	if err != nil {
		result.Unavailable = append(result.Unavailable, "scratch")
	}
	return result
}

func BenchmarkReplay(b *testing.B) {
	for _, backend := range []string{"pebble", "bbolt", "badger"} {
		b.Run(backend, func(b *testing.B) {
			for _, kind := range []string{"overlay", "markers", "directories"} {
				b.Run(kind, func(b *testing.B) { replay(b, backend, kind) })
			}
		})
	}
}

func replay(b *testing.B, backend, kind string) {
	if b.N != 1 {
		b.Fatal("replay requires -benchtime=1x for aligned observations")
	}
	fixture := newReplayFixture(kind)
	inputHash := sha256.New()
	for _, round := range fixture.rounds {
		_, _ = inputHash.Write([]byte(entriesDigest(round)))
	}
	inputDigest := hex.EncodeToString(inputHash.Sum(nil))
	expectedDigest := entriesDigest(fixture.final)
	root := filepath.Join(b.TempDir(), "store")
	if err := os.Mkdir(root, 0700); err != nil {
		b.Fatal(err)
	}
	before := telemetry.ReadWorkingRuntime()
	resourcesBefore := readReplayResources(root)
	latencies := make(map[string][]int64)
	timed := func(operation string, call func() error) {
		started := time.Now()
		err := call()
		latencies[operation] = append(latencies[operation], time.Since(started).Nanoseconds())
		if err != nil {
			b.Fatal(err)
		}
	}
	var store *Store
	timed("open", func() error { var err error; store, err = Open(backend, root); return err })
	defer store.Close()
	var mutex sync.Mutex
	peak := readReplayResources(root)
	observe := func() {
		now := readReplayResources(root)
		mutex.Lock()
		defer mutex.Unlock()
		peak.RSSBytes = max(peak.RSSBytes, now.RSSBytes)
		peak.MappedStoreBytes = max(peak.MappedStoreBytes, now.MappedStoreBytes)
		peak.ScratchBytes = max(peak.ScratchBytes, now.ScratchBytes)
		peak.Unavailable = append(peak.Unavailable, now.Unavailable...)
	}
	done, stopped := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				observe()
			case <-done:
				return
			}
		}
	}()
	defer func() { close(done); <-stopped }()
	b.ResetTimer()
	for _, round := range fixture.rounds {
		for first := 0; first < len(round); {
			last, size := first, 0
			for last < len(round) && size+len(round[last].Key)+len(round[last].Value)+16 <= MaxBatchBytes {
				size += len(round[last].Key) + len(round[last].Value) + 16
				last++
			}
			if last == first {
				b.Fatal("fixture exceeds single-entry bound")
			}
			timed("put", func() error { return store.Put(b.Context(), round[first:last]) })
			observe()
			first = last
		}
	}
	for _, entry := range fixture.final {
		var value []byte
		timed("get", func() error {
			var found bool
			var err error
			value, found, err = store.Get(b.Context(), entry.Key)
			if err == nil && !found {
				return fmt.Errorf("fixture entry missing")
			}
			return err
		})
		aad := entry.Key
		if kind != "overlay" {
			aad = append([]byte("v1/"+kind+"/"), entry.Key...)
		}
		plain, err := fixture.aead.Open(nil, value[:fixture.aead.NonceSize()], value[fixture.aead.NonceSize():], aad)
		if err != nil || !bytes.Equal(plain, fixture.plain[string(entry.Key)]) {
			b.Fatal("codec/semantic mismatch", err)
		}
	}
	var actual []Entry
	var after []byte
	for {
		var page []Entry
		timed("scan", func() error { var err error; page, err = store.Scan(b.Context(), nil, after, 8); return err })
		if len(page) == 0 {
			break
		}
		actual = append(actual, page...)
		after = page[len(page)-1].Key
	}
	resultDigest := entriesDigest(actual)
	if expectedDigest != resultDigest {
		b.Fatal("ordered replay digest mismatch")
	}
	if kind == "overlay" {
		rows, err := store.Scan(b.Context(), fixture.final[0].Key[:32], nil, 8)
		if err != nil || len(rows) != 2 {
			b.Fatal("duplicate-location prefix semantics", err)
		}
	}
	observe()
	timed("close", store.Close)
	b.StopTimer()
	resourcesAfter := readReplayResources(root)
	afterRuntime := telemetry.ReadWorkingRuntime()
	mutex.Lock()
	sampledPeak := peak
	mutex.Unlock()
	var encoded uint64
	for _, entry := range fixture.final {
		encoded += uint64(len(entry.Key) + len(entry.Value))
	}
	record := struct {
		Backend         string                           `json:"backend"`
		Workload        string                           `json:"workload"`
		Entries         int                              `json:"entries"`
		EncodedBytes    uint64                           `json:"encoded_bytes"`
		InputDigest     string                           `json:"input_sha256"`
		ExpectedDigest  string                           `json:"expected_result_sha256"`
		ResultDigest    string                           `json:"result_sha256"`
		Latencies       map[string][]int64               `json:"latencies_ns"`
		Before          telemetry.WorkingRuntimeSnapshot `json:"runtime_before"`
		After           telemetry.WorkingRuntimeSnapshot `json:"runtime_after"`
		ResourcesBefore replayResources                  `json:"resources_before"`
		ResourcesAfter  replayResources                  `json:"resources_after"`
		Peak            replayResources                  `json:"sampled_peak"`
	}{backend, kind, len(actual), encoded, inputDigest, expectedDigest, resultDigest, latencies, before, afterRuntime, resourcesBefore, resourcesAfter, sampledPeak}
	jsonRecord, err := json.Marshal(record)
	if err != nil {
		b.Fatal(err)
	}
	b.Logf("phase35_m1=%s", jsonRecord)
	if err := os.RemoveAll(root); err != nil {
		b.Fatal(err)
	}
}
