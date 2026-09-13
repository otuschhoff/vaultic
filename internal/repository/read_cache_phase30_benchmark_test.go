package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/backend/local"
	"github.com/otuschhoff/vaultic/internal/backend/mem"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

const (
	phase30BenchBlobSize    = 1 << 20
	phase30BenchRequestSize = 64 << 10
	phase30BenchBudgetBytes = 64 << 20
)

var phase30BenchByteSink byte

func BenchmarkPhase30ReadCacheRepresentations(b *testing.B) {
	backendModes := []string{"mem", "local"}
	backendFilter := phase30BenchmarkBackendFilter()
	chunkSizes := []int{1 << 20, 4 << 20, 8 << 20, 16 << 20}

	for _, backendMode := range backendModes {
		if len(backendFilter) != 0 {
			if _, ok := backendFilter[backendMode]; !ok {
				continue
			}
		}
		for _, chunkSize := range chunkSizes {
			namePrefix := fmt.Sprintf("backend=%s/chunk=%dMiB", backendMode, chunkSize>>20)

			b.Run(namePrefix+"/source-encrypted-range/cold-fill/sequential", func(b *testing.B) {
				fixture := newPhase30BenchFixture(b, backendMode, chunkSize)
				benchSourceEncryptedRange(b, fixture, true)
			})
			b.Run(namePrefix+"/source-encrypted-range/warm-hit/sequential", func(b *testing.B) {
				fixture := newPhase30BenchFixture(b, backendMode, chunkSize)
				benchSourceEncryptedRange(b, fixture, false)
			})

			b.Run(namePrefix+"/compressed-derived/cold-fill/random", func(b *testing.B) {
				fixture := newPhase30BenchFixture(b, backendMode, chunkSize)
				benchBlobRepresentation(b, fixture, readCacheRepCompressedContainer, true, true)
			})
			b.Run(namePrefix+"/compressed-derived/warm-hit/random", func(b *testing.B) {
				fixture := newPhase30BenchFixture(b, backendMode, chunkSize)
				benchBlobRepresentation(b, fixture, readCacheRepCompressedContainer, false, true)
			})

			b.Run(namePrefix+"/decoded-blob-extent/cold-fill/sequential", func(b *testing.B) {
				fixture := newPhase30BenchFixture(b, backendMode, chunkSize)
				benchBlobRepresentation(b, fixture, readCacheRepDecodedExtent, true, false)
			})
			b.Run(namePrefix+"/decoded-blob-extent/warm-hit/sequential", func(b *testing.B) {
				fixture := newPhase30BenchFixture(b, backendMode, chunkSize)
				benchBlobRepresentation(b, fixture, readCacheRepDecodedExtent, false, false)
			})

			b.Run(namePrefix+"/decoded-extent/warm-hit/sequential", func(b *testing.B) {
				fixture := newPhase30BenchFixture(b, backendMode, chunkSize)
				benchLogicalExtent(b, fixture, "sequential", false)
			})
			b.Run(namePrefix+"/decoded-extent/warm-hit/sparse-random", func(b *testing.B) {
				fixture := newPhase30BenchFixture(b, backendMode, chunkSize)
				benchLogicalExtent(b, fixture, "sparse-random", false)
			})
			b.Run(namePrefix+"/decoded-extent/warm-hit/shared-blobs", func(b *testing.B) {
				fixture := newPhase30BenchFixture(b, backendMode, chunkSize)
				benchLogicalExtent(b, fixture, "shared-blobs", false)
			})
			b.Run(namePrefix+"/decoded-extent/cold-fill/mixed-hot-cold", func(b *testing.B) {
				fixture := newPhase30BenchFixture(b, backendMode, chunkSize)
				benchLogicalExtent(b, fixture, "mixed-hot-cold", true)
			})

			b.Run(namePrefix+"/whole-file/cold-fill/sequential", func(b *testing.B) {
				fixture := newPhase30BenchFixture(b, backendMode, chunkSize)
				benchWholeFile(b, fixture, true)
			})
			b.Run(namePrefix+"/whole-file/warm-hit/random", func(b *testing.B) {
				fixture := newPhase30BenchFixture(b, backendMode, chunkSize)
				benchWholeFile(b, fixture, false)
			})
		}
	}
}

func phase30BenchmarkBackendFilter() map[string]struct{} {
	raw := strings.TrimSpace(os.Getenv("VAULTIC_PHASE30_BENCH_BACKENDS"))
	if raw == "" {
		return nil
	}
	allowed := make(map[string]struct{})
	for _, item := range strings.Split(raw, ",") {
		name := strings.ToLower(strings.TrimSpace(item))
		if name == "" {
			continue
		}
		allowed[name] = struct{}{}
	}
	if len(allowed) == 0 {
		return nil
	}
	return allowed
}

type phase30OriginMeter struct {
	backend.Backend
	cas      backend.ConditionalWriter
	requests atomic.Uint64
	bytes    atomic.Uint64
}

type phase30CountingReader struct {
	reader io.Reader
	n      uint64
}

func (reader *phase30CountingReader) Read(buf []byte) (int, error) {
	read, err := reader.reader.Read(buf)
	reader.n += uint64(read)
	return read, err
}

func (meter *phase30OriginMeter) Load(ctx context.Context, handle backend.Handle, length int, offset int64, fn func(io.Reader) error) error {
	meter.requests.Add(1)
	return meter.Backend.Load(ctx, handle, length, offset, func(reader io.Reader) error {
		counter := &phase30CountingReader{reader: reader}
		err := fn(counter)
		meter.bytes.Add(counter.n)
		return err
	})
}

func (meter *phase30OriginMeter) Unwrap() backend.Backend {
	return meter.Backend
}

func (meter *phase30OriginMeter) CompareAndSwap(ctx context.Context, handle backend.Handle, expected []byte, replacement []byte) ([]byte, bool, error) {
	if meter.cas == nil {
		return nil, false, backend.ErrConditionalWriteUnsupported
	}
	return meter.cas.CompareAndSwap(ctx, handle, expected, replacement)
}

func (meter *phase30OriginMeter) snapshot() (uint64, uint64) {
	return meter.requests.Load(), meter.bytes.Load()
}

type phase30BenchFixture struct {
	repo *Repository

	origin    *phase30OriginMeter
	cache     backend.Backend
	chunkSize int

	blobOrder []vaultic.BlobHandle
	blobBytes map[vaultic.ID][]byte

	sourcePack   backend.Handle
	sourceOffset int64
	sourceLength int
	sourceBytes  []byte

	wholeContent []vaultic.ID
	wholeCumSize []uint64
	wholeTotal   uint64

	largeContent []vaultic.ID
	largeCumSize []uint64
	largeTotal   uint64

	sharedAContent []vaultic.ID
	sharedACumSize []uint64
	sharedATotal   uint64

	sharedBContent []vaultic.ID
	sharedBCumSize []uint64
	sharedBTotal   uint64

	coldContent []vaultic.ID
	coldCumSize []uint64
	coldTotal   uint64
}

func newPhase30BenchFixture(b *testing.B, backendMode string, chunkSize int) *phase30BenchFixture {
	b.Helper()

	var primary backend.Backend
	var cacheBackend backend.Backend
	switch backendMode {
	case "mem":
		primary = mem.New()
		cacheBackend = mem.New()
	case "local":
		root := b.TempDir()
		primaryDir := filepath.Join(root, "primary")
		cacheDir := filepath.Join(root, "cache")
		primaryLocal, err := local.Create(context.Background(), local.Config{Path: primaryDir, Connections: 2}, b.Logf)
		if err != nil {
			b.Fatalf("create local primary backend: %v", err)
		}
		cacheLocal, err := local.Create(context.Background(), local.Config{Path: cacheDir, Connections: 2}, b.Logf)
		if err != nil {
			b.Fatalf("create local read-cache backend: %v", err)
		}
		primary = primaryLocal
		cacheBackend = cacheLocal
	default:
		b.Fatalf("unsupported backend mode %q", backendMode)
	}
	if primary.Properties().Connections == 0 || cacheBackend.Properties().Connections == 0 {
		b.Fatal("benchmark backends must provide at least one connection")
	}

	meter := &phase30OriginMeter{Backend: primary, cas: backend.AsCapability[backend.ConditionalWriter](primary)}
	repo, _ := TestRepositoryWithBackend(b, meter, vaultic.StableRepoVersion, Options{})
	b.Cleanup(func() {
		if err := repo.Close(); err != nil {
			b.Errorf("close benchmark repository: %v", err)
		}
	})
	cfg := repo.Config()
	cfg.PlacementBackends = []vaultic.PlacementBackend{
		{ID: "primary", Role: PlacementRolePrimary, FailureDomain: "primary", CapacityBytes: phase30BenchBudgetBytes},
		{
			ID: "rc", Role: PlacementRoleReadCache, FailureDomain: "cache",
			CapacityBytes: phase30BenchBudgetBytes, TargetPackSizeBytes: uint64(chunkSize),
			ReadCacheTrust: readCacheTrustPlaintext, ReadCacheAck: true,
		},
	}
	repo.setConfig(cfg)
	repo.AttachPlacementBackend(PlacementBackendHash("primary"), meter)
	repo.AttachPlacementBackend(PlacementBackendHash("rc"), cacheBackend)

	blobOrder := make([]vaultic.BlobHandle, 0, 12)
	blobBytes := make(map[vaultic.ID][]byte, 12)
	payloads := phase30BenchmarkPayloads()
	if err := repo.WithBlobUploader(context.Background(), func(ctx context.Context, uploader vaultic.BlobSaverWithAsync) error {
		for _, payload := range payloads {
			id, _, _, err := uploader.SaveBlob(ctx, vaultic.DataBlob, payload, vaultic.ID{}, true)
			if err != nil {
				return err
			}
			blobOrder = append(blobOrder, vaultic.BlobHandle{Type: vaultic.DataBlob, ID: id})
			blobBytes[id] = payload
		}
		return nil
	}); err != nil {
		b.Fatalf("save benchmark blobs: %v", err)
	}
	if err := repo.flush(context.Background()); err != nil {
		b.Fatalf("flush benchmark packs: %v", err)
	}
	if len(blobOrder) < 8 {
		b.Fatalf("unexpected benchmark blob count %d", len(blobOrder))
	}

	engine, err := repo.legacyIndexEngine()
	if err != nil {
		b.Fatalf("load legacy index engine: %v", err)
	}
	lookup := engine.Lookup(blobOrder[0])
	if len(lookup) == 0 {
		b.Fatal("missing packed blob mapping")
	}
	sourcePack := backend.Handle{Type: backend.PackFile, Name: lookup[0].PackID().String()}
	sourceOffset := int64(lookup[0].Blob.Offset)
	sourceLength := int(lookup[0].Blob.Length)
	sourceBytes, err := loadBytes(context.Background(), meter.Backend, sourcePack, sourceLength, sourceOffset)
	if err != nil {
		b.Fatalf("load source encrypted range bytes: %v", err)
	}

	wholeContent := []vaultic.ID{blobOrder[0].ID, blobOrder[1].ID, blobOrder[2].ID, blobOrder[3].ID, blobOrder[4].ID, blobOrder[5].ID}
	wholeCum := phase30CumSizes(blobBytes, wholeContent)
	wholeTotal := wholeCum[len(wholeCum)-1]
	if wholeTotal > 8*1024*1024 {
		b.Fatalf("whole-file fixture too large for whole-file promotion: %d", wholeTotal)
	}

	largeContent := make([]vaultic.ID, 0, len(blobOrder)*3)
	for i := 0; i < 3; i++ {
		for _, handle := range blobOrder {
			largeContent = append(largeContent, handle.ID)
		}
	}
	largeCum := phase30CumSizes(blobBytes, largeContent)
	largeTotal := largeCum[len(largeCum)-1]

	sharedA := []vaultic.ID{blobOrder[0].ID, blobOrder[1].ID, blobOrder[2].ID, blobOrder[3].ID, blobOrder[0].ID, blobOrder[1].ID}
	sharedB := []vaultic.ID{blobOrder[0].ID, blobOrder[1].ID, blobOrder[6].ID, blobOrder[7].ID, blobOrder[0].ID, blobOrder[1].ID}
	sharedACum := phase30CumSizes(blobBytes, sharedA)
	sharedBCum := phase30CumSizes(blobBytes, sharedB)

	coldContent := []vaultic.ID{blobOrder[8].ID, blobOrder[9].ID, blobOrder[10].ID, blobOrder[11].ID, blobOrder[8].ID, blobOrder[9].ID}
	coldCum := phase30CumSizes(blobBytes, coldContent)

	return &phase30BenchFixture{
		repo:           repo,
		origin:         meter,
		cache:          cacheBackend,
		chunkSize:      chunkSize,
		blobOrder:      blobOrder,
		blobBytes:      blobBytes,
		sourcePack:     sourcePack,
		sourceOffset:   sourceOffset,
		sourceLength:   sourceLength,
		sourceBytes:    sourceBytes,
		wholeContent:   wholeContent,
		wholeCumSize:   wholeCum,
		wholeTotal:     wholeTotal,
		largeContent:   largeContent,
		largeCumSize:   largeCum,
		largeTotal:     largeTotal,
		sharedAContent: sharedA,
		sharedACumSize: sharedACum,
		sharedATotal:   sharedACum[len(sharedACum)-1],
		sharedBContent: sharedB,
		sharedBCumSize: sharedBCum,
		sharedBTotal:   sharedBCum[len(sharedBCum)-1],
		coldContent:    coldContent,
		coldCumSize:    coldCum,
		coldTotal:      coldCum[len(coldCum)-1],
	}
}

func phase30BenchmarkPayloads() [][]byte {
	payloads := make([][]byte, 0, 12)
	seeded := rand.New(rand.NewSource(30))
	for i := 0; i < 12; i++ {
		buf := make([]byte, phase30BenchBlobSize)
		if i%2 == 0 {
			for j := range buf {
				buf[j] = byte('A' + (j+i)%23)
			}
		} else {
			for j := range buf {
				buf[j] = byte(seeded.Intn(256))
			}
		}
		payloads = append(payloads, buf)
	}
	return payloads
}

func phase30CumSizes(blobData map[vaultic.ID][]byte, content []vaultic.ID) []uint64 {
	cum := make([]uint64, len(content)+1)
	for i, id := range content {
		cum[i+1] = cum[i] + uint64(len(blobData[id]))
	}
	return cum
}

func benchSourceEncryptedRange(b *testing.B, fixture *phase30BenchFixture, coldFill bool) {
	b.Helper()
	expected := append([]byte(nil), fixture.sourceBytes...)
	buf := make([]byte, fixture.sourceLength)

	if !coldFill {
		if _, err := fixture.repo.readPackAtFromPlacements(context.Background(), fixture.sourcePack, fixture.sourceOffset, buf); err != nil {
			b.Fatalf("warm-up source encrypted range: %v", err)
		}
		if !bytes.Equal(buf, expected) {
			b.Fatal("warm-up source encrypted range mismatch")
		}
	}

	beforeReq, beforeBytes := fixture.origin.snapshot()
	b.ReportAllocs()
	b.SetBytes(int64(len(expected)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if coldFill {
			if err := fixture.repo.ClearReadCache(context.Background(), "rc"); err != nil {
				b.Fatalf("clear cache for cold fill: %v", err)
			}
		}
		_, err := fixture.repo.readPackAtFromPlacements(context.Background(), fixture.sourcePack, fixture.sourceOffset, buf)
		if err != nil {
			b.Fatalf("source encrypted range read: %v", err)
		}
		phase30BenchByteSink ^= buf[0]
	}
	b.StopTimer()

	if _, err := fixture.repo.readPackAtFromPlacements(context.Background(), fixture.sourcePack, fixture.sourceOffset, buf); err != nil {
		b.Fatalf("post-run source encrypted range verification read: %v", err)
	}
	if !bytes.Equal(buf, expected) {
		b.Fatal("post-run source encrypted range mismatch")
	}

	afterReq, afterBytes := fixture.origin.snapshot()
	status := fixture.repo.ReadCacheStatus()
	usefulBytes := float64(uint64(len(expected)) * uint64(b.N))
	reportPhase30BenchMetrics(b, usefulBytes, status.UsedBytes, afterReq-beforeReq, afterBytes-beforeBytes, float64(len(expected)), 0)
}

func benchBlobRepresentation(b *testing.B, fixture *phase30BenchFixture, representation string, coldFill bool, randomOrder bool) {
	b.Helper()
	ids := append([]vaultic.BlobHandle(nil), fixture.blobOrder...)
	if randomOrder {
		phase30ShuffleHandles(ids)
	}

	warmAndVerify := func() {
		for _, handle := range ids[:4] {
			got, err := fixture.repo.LoadBlob(context.Background(), handle, nil)
			if err != nil {
				b.Fatalf("warm-up blob load %s: %v", handle.ID, err)
			}
			if !bytes.Equal(got, fixture.blobBytes[handle.ID]) {
				b.Fatalf("warm-up blob mismatch for %s", handle.ID)
			}
		}
		if representation == readCacheRepCompressedContainer {
			if err := phase30RemoveRepresentation(fixture.cache, readCacheRepDecodedExtent); err != nil {
				b.Fatalf("remove decoded representation before compressed benchmark: %v", err)
			}
		}
	}

	warmAndVerify()

	beforeReq, beforeBytes := fixture.origin.snapshot()
	b.ReportAllocs()
	b.SetBytes(int64(phase30BenchBlobSize))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if coldFill {
			if err := fixture.repo.ClearReadCache(context.Background(), "rc"); err != nil {
				b.Fatalf("clear cache for cold fill: %v", err)
			}
			warmAndVerify()
		}
		handle := ids[i%len(ids)]
		got, err := fixture.repo.LoadBlob(context.Background(), handle, nil)
		if err != nil {
			b.Fatalf("benchmark blob load %s: %v", handle.ID, err)
		}
		phase30BenchByteSink ^= got[0]
	}
	b.StopTimer()

	for _, handle := range ids[:2] {
		got, err := fixture.repo.LoadBlob(context.Background(), handle, nil)
		if err != nil {
			b.Fatalf("post-run blob verification read: %v", err)
		}
		if !bytes.Equal(got, fixture.blobBytes[handle.ID]) {
			b.Fatalf("post-run blob mismatch for %s", handle.ID)
		}
	}

	afterReq, afterBytes := fixture.origin.snapshot()
	status := fixture.repo.ReadCacheStatus()
	verifyProxy := float64(phase30BenchBlobSize)
	decodeProxy := 0.0
	if representation == readCacheRepCompressedContainer {
		decodeProxy = float64(phase30BenchBlobSize)
	}
	usefulBytes := float64(uint64(phase30BenchBlobSize) * uint64(b.N))
	reportPhase30BenchMetrics(b, usefulBytes, status.UsedBytes, afterReq-beforeReq, afterBytes-beforeBytes, verifyProxy, decodeProxy)
}

func benchLogicalExtent(b *testing.B, fixture *phase30BenchFixture, workload string, coldFill bool) {
	b.Helper()
	request := make([]byte, phase30BenchRequestSize)

	warm := func(content []vaultic.ID, cum []uint64, total uint64) {
		offset := uint64(0)
		if total > uint64(len(request)) {
			offset = total/2 - uint64(len(request))/2
		}
		for i := 0; i < 2; i++ {
			readBytes, err := fixture.repo.ReadLogicalFileRange(context.Background(), content, cum, offset, request)
			if err != nil {
				b.Fatalf("logical warm-up read: %v", err)
			}
			if readBytes != len(request) {
				b.Fatalf("logical warm-up read bytes = %d, want %d", readBytes, len(request))
			}
		}
	}

	warm(fixture.largeContent, fixture.largeCumSize, fixture.largeTotal)
	warm(fixture.sharedAContent, fixture.sharedACumSize, fixture.sharedATotal)
	warm(fixture.sharedBContent, fixture.sharedBCumSize, fixture.sharedBTotal)

	randOffsets := phase30RandomOffsets(fixture.largeTotal, uint64(len(request)), b.N+32, 3030)
	sharedSwitch := 0

	beforeReq, beforeBytes := fixture.origin.snapshot()
	b.ReportAllocs()
	b.SetBytes(int64(len(request)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if coldFill {
			if err := fixture.repo.ClearReadCache(context.Background(), "rc"); err != nil {
				b.Fatalf("clear cache for cold fill: %v", err)
			}
		}

		var content []vaultic.ID
		var cum []uint64
		var total uint64
		var offset uint64

		switch workload {
		case "sequential":
			content = fixture.largeContent
			cum = fixture.largeCumSize
			total = fixture.largeTotal
			offset = phase30SequentialOffset(i, total, uint64(len(request)))
		case "sparse-random":
			content = fixture.largeContent
			cum = fixture.largeCumSize
			offset = randOffsets[i%len(randOffsets)]
		case "shared-blobs":
			if i%2 == 0 {
				content = fixture.sharedAContent
				cum = fixture.sharedACumSize
				total = fixture.sharedATotal
			} else {
				content = fixture.sharedBContent
				cum = fixture.sharedBCumSize
				total = fixture.sharedBTotal
			}
			offset = phase30SequentialOffset(sharedSwitch, total, uint64(len(request)))
			sharedSwitch++
		case "mixed-hot-cold":
			if i%3 == 0 {
				content = fixture.coldContent
				cum = fixture.coldCumSize
				total = fixture.coldTotal
			} else {
				content = fixture.largeContent
				cum = fixture.largeCumSize
				total = fixture.largeTotal
			}
			offset = randOffsets[i%len(randOffsets)] % maxUint64(1, total-uint64(len(request))+1)
		default:
			b.Fatalf("unsupported logical workload %q", workload)
		}

		readBytes, err := fixture.repo.ReadLogicalFileRange(context.Background(), content, cum, offset, request)
		if err != nil {
			b.Fatalf("logical workload read: %v", err)
		}
		if readBytes != len(request) {
			b.Fatalf("logical workload read bytes = %d, want %d", readBytes, len(request))
		}
		phase30BenchByteSink ^= request[0]
	}
	b.StopTimer()

	verify := make([]byte, len(request))
	if _, err := fixture.repo.ReadLogicalFileRange(context.Background(), fixture.sharedAContent, fixture.sharedACumSize, 0, verify); err != nil {
		b.Fatalf("post-run logical verification read: %v", err)
	}

	afterReq, afterBytes := fixture.origin.snapshot()
	status := fixture.repo.ReadCacheStatus()
	usefulBytes := float64(uint64(len(request)) * uint64(b.N))
	reportPhase30BenchMetrics(b, usefulBytes, status.UsedBytes, afterReq-beforeReq, afterBytes-beforeBytes, float64(len(request)), float64(len(request)))
}

func benchWholeFile(b *testing.B, fixture *phase30BenchFixture, coldFill bool) {
	b.Helper()
	request := make([]byte, phase30BenchRequestSize)
	if uint64(len(request)) >= fixture.wholeTotal {
		request = make([]byte, fixture.wholeTotal/2)
	}

	warmWhole := func() {
		offset := uint64(0)
		for i := 0; i < 2; i++ {
			readBytes, err := fixture.repo.ReadLogicalFileRange(context.Background(), fixture.wholeContent, fixture.wholeCumSize, offset, request)
			if err != nil {
				b.Fatalf("whole-file warm-up read: %v", err)
			}
			if readBytes != len(request) {
				b.Fatalf("whole-file warm-up read bytes = %d, want %d", readBytes, len(request))
			}
		}
	}

	warmWhole()
	readOffsets := phase30RandomOffsets(fixture.wholeTotal, uint64(len(request)), b.N+32, 3130)

	beforeReq, beforeBytes := fixture.origin.snapshot()
	b.ReportAllocs()
	b.SetBytes(int64(len(request)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if coldFill {
			if err := fixture.repo.ClearReadCache(context.Background(), "rc"); err != nil {
				b.Fatalf("clear cache for cold fill: %v", err)
			}
			warmWhole()
		}
		readBytes, err := fixture.repo.ReadLogicalFileRange(
			context.Background(), fixture.wholeContent, fixture.wholeCumSize,
			readOffsets[i%len(readOffsets)], request,
		)
		if err != nil {
			b.Fatalf("whole-file benchmark read: %v", err)
		}
		if readBytes != len(request) {
			b.Fatalf("whole-file read bytes = %d, want %d", readBytes, len(request))
		}
		phase30BenchByteSink ^= request[0]
	}
	b.StopTimer()

	verify := make([]byte, len(request))
	if _, err := fixture.repo.ReadLogicalFileRange(context.Background(), fixture.wholeContent, fixture.wholeCumSize, 0, verify); err != nil {
		b.Fatalf("post-run whole-file verification read: %v", err)
	}

	afterReq, afterBytes := fixture.origin.snapshot()
	status := fixture.repo.ReadCacheStatus()
	usefulBytes := float64(uint64(len(request)) * uint64(b.N))
	reportPhase30BenchMetrics(b, usefulBytes, status.UsedBytes, afterReq-beforeReq, afterBytes-beforeBytes, float64(len(request)), 0)
}

func phase30ShuffleHandles(handles []vaultic.BlobHandle) {
	randSource := rand.New(rand.NewSource(777))
	randSource.Shuffle(len(handles), func(i, j int) {
		handles[i], handles[j] = handles[j], handles[i]
	})
}

func phase30RandomOffsets(total uint64, length uint64, count int, seed int64) []uint64 {
	if count <= 0 {
		count = 1
	}
	out := make([]uint64, count)
	if total <= length {
		return out
	}
	limit := total - length
	randSource := rand.New(rand.NewSource(seed))
	for i := range out {
		out[i] = uint64(randSource.Int63n(int64(limit + 1)))
	}
	return out
}

func phase30SequentialOffset(iter int, total uint64, length uint64) uint64 {
	if total <= length {
		return 0
	}
	limit := total - length
	stride := uint64(phase30BenchRequestSize)
	return (uint64(iter) * stride) % (limit + 1)
}

func reportPhase30BenchMetrics(
	b *testing.B,
	usefulBytes float64,
	occupiedBytes uint64,
	originRequests uint64,
	originBytes uint64,
	verifyProxy float64,
	decodeProxy float64,
) {
	b.Helper()
	if occupiedBytes == 0 {
		occupiedBytes = 1
	}
	if b.N <= 0 {
		return
	}
	ops := float64(b.N)
	b.ReportMetric(usefulBytes/float64(occupiedBytes), "useful_B/occ_B")
	b.ReportMetric(float64(originBytes)/ops, "origin_B/op")
	b.ReportMetric(float64(originRequests)/ops, "origin_req/op")
	if originRequests > 0 {
		b.ReportMetric(float64(originBytes)/float64(originRequests), "origin_B/req")
	}
	b.ReportMetric(verifyProxy, "verify_proxy_B/op")
	b.ReportMetric(decodeProxy, "decode_proxy_B/op")
}

func phase30RemoveRepresentation(cacheBackend backend.Backend, representation string) error {
	var handles []backend.Handle
	err := cacheBackend.List(context.Background(), backend.StagingFile, func(info backend.FileInfo) error {
		if !strings.HasSuffix(info.Name, ".json") {
			return nil
		}
		metaHandle := backend.Handle{Type: backend.StagingFile, Name: info.Name}
		raw, err := loadAll(cacheBackend, metaHandle)
		if err != nil {
			return err
		}
		var meta readCacheChunkMeta
		if err := json.Unmarshal(raw, &meta); err != nil {
			return nil
		}
		if meta.Identity.Representation != representation {
			return nil
		}
		dataName := strings.TrimSuffix(info.Name, ".json") + ".bin"
		handles = append(handles,
			metaHandle,
			backend.Handle{Type: backend.StagingFile, Name: dataName},
		)
		return nil
	})
	if err != nil {
		return err
	}
	for _, handle := range handles {
		if err := cacheBackend.Remove(context.Background(), handle); err != nil && !cacheBackend.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func maxUint64(left uint64, right uint64) uint64 {
	if left > right {
		return left
	}
	return right
}
