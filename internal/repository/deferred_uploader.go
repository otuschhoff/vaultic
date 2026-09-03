package repository

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sync"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/repository/hashing"
	"github.com/otuschhoff/vaultic/internal/repository/staging"
	"github.com/otuschhoff/vaultic/internal/vaultic"
	"golang.org/x/sync/errgroup"
)

type DeferredBackend struct {
	ID            string
	FailureDomain string
	Offsite       bool
	Backend       backend.Backend
}

type DeferredUploadOptions struct {
	Backends           []DeferredBackend
	Policy             staging.Policy
	MaxAdditionalBytes uint64
}

type DeferredUploadResult struct {
	Packs   []staging.Pack
	Records []staging.Record
}

func (r *Repository) DeferredUploadPlan() (DeferredUploadOptions, staging.Store, error) {
	if len(r.cfg.StagingBackends) == 0 {
		return DeferredUploadOptions{}, staging.Store{}, fmt.Errorf("repository has no staging backends")
	}
	configured := make(map[string]vaultic.PlacementBackend, len(r.cfg.PlacementBackends))
	for _, placement := range r.cfg.PlacementBackends {
		configured[placement.ID] = placement
	}
	policy := staging.Policy{MinCopies: r.cfg.PlacementPolicy.MinCopies, MinDomains: r.cfg.PlacementPolicy.MinDomains, MinOffsite: r.cfg.PlacementPolicy.MinOffsite}
	options := DeferredUploadOptions{Policy: policy}
	mirrors := make(map[string]backend.Backend, len(r.cfg.StagingBackends))
	mirrorPlacements := make(map[string]staging.MirrorPlacement, len(r.cfg.StagingBackends))
	for _, id := range r.cfg.StagingBackends {
		placement, ok := configured[id]
		if !ok {
			return DeferredUploadOptions{}, staging.Store{}, fmt.Errorf("staging backend %q is not configured", id)
		}
		destination, ok := r.placementBackend(PlacementBackendHash(id))
		if !ok {
			continue
		}
		options.Backends = append(options.Backends, DeferredBackend{ID: id, FailureDomain: placement.FailureDomain, Offsite: placement.Offsite, Backend: destination})
		mirrors[id] = destination
		mirrorPlacements[id] = staging.MirrorPlacement{FailureDomain: placement.FailureDomain, Offsite: placement.Offsite}
	}
	if len(options.Backends) == 0 {
		return DeferredUploadOptions{}, staging.Store{}, fmt.Errorf("no staging backend is reachable")
	}
	domains := make(map[string]struct{})
	offsite := uint(0)
	for _, destination := range options.Backends {
		domains[destination.FailureDomain] = struct{}{}
		if destination.Offsite {
			offsite++
		}
	}
	if uint(len(options.Backends)) < policy.MinCopies || uint(len(domains)) < policy.MinDomains || offsite < policy.MinOffsite {
		return DeferredUploadOptions{}, staging.Store{}, fmt.Errorf("reachable staging backends do not satisfy placement policy")
	}
	journalKey, err := staging.DeriveJournalKey(r.key.EncryptionKey[:], r.cfg.ID)
	if err != nil {
		return DeferredUploadOptions{}, staging.Store{}, err
	}
	return options, staging.Store{Mirrors: mirrors, MirrorPlacements: mirrorPlacements, Key: journalKey, Policy: policy}, nil
}

type deferredPackSink struct {
	options       DeferredUploadOptions
	mu            sync.Mutex
	reservedBytes uint64
	result        DeferredUploadResult
}

func (r *Repository) WithDeferredBlobUploader(ctx context.Context, options DeferredUploadOptions, fn func(context.Context, vaultic.BlobSaverWithAsync) error) (DeferredUploadResult, error) {
	if len(options.Backends) == 0 {
		return DeferredUploadResult{}, fmt.Errorf("deferred upload requires placement backends")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	wg, ctx := errgroup.WithContext(ctx)
	wg.SetLimit(2 + r.packerCount)
	r.mainWg = wg
	r.blobSaver = &sync.WaitGroup{}
	sink := &deferredPackSink{options: options}
	r.startPackUploaderWith(ctx, wg, sink)
	wg.Go(func() error {
		if err := fn(ctx, &deferredBlobSaver{repo: r}); err != nil {
			return err
		}
		r.flushBlobSaver()
		r.mainWg = nil
		return r.flushPackUploader(ctx)
	})
	if err := wg.Wait(); err != nil {
		return DeferredUploadResult{}, err
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	return sink.result, nil
}

func (r *Repository) startPackUploaderWith(ctx context.Context, wg *errgroup.Group, sink savePacker) {
	innerWg, innerCtx := errgroup.WithContext(ctx)
	r.packerWg = innerWg
	r.uploader = newPackerUploader(innerCtx, innerWg, sink, r.Connections())
	treeSize, treeLimit, treeGrow := r.packSizing(vaultic.TreeBlob)
	dataSize, dataLimit, dataGrow := r.packSizing(vaultic.DataBlob)
	r.treePM = newConfiguredPackerManager(r.key, vaultic.TreeBlob, treeSize, treeLimit, 0, treeGrow, r.packerCount, r.uploader.QueuePacker)
	r.dataPM = newConfiguredPackerManager(r.key, vaultic.DataBlob, dataSize, dataLimit, 0, dataGrow, r.packerCount, r.uploader.QueuePacker)
	wg.Go(innerWg.Wait)
}

type deferredBlobSaver struct{ repo *Repository }

func (s *deferredBlobSaver) SaveBlob(ctx context.Context, blobType vaultic.BlobType, data []byte, id vaultic.ID, _ bool) (vaultic.ID, bool, int, error) {
	if int64(len(data)) > math.MaxUint32 {
		return vaultic.ID{}, false, 0, fmt.Errorf("blob is larger than 4GB")
	}
	if id.IsNull() {
		id = vaultic.Hash(data)
	}
	size, err := s.repo.saveAndEncrypt(ctx, blobType, data, id)
	return id, false, size, err
}

func (s *deferredBlobSaver) SaveBlobAsync(ctx context.Context, blobType vaultic.BlobType, data []byte, id vaultic.ID, storeDuplicate bool, callback func(vaultic.ID, bool, int, error)) {
	s.repo.mainWg.Go(func() error {
		newID, known, size, err := s.SaveBlob(ctx, blobType, data, id, storeDuplicate)
		callback(newID, known, size, err)
		return err
	})
}

func (sink *deferredPackSink) savePacker(ctx context.Context, blobType vaultic.BlobType, packer *packer) error {
	defer packer.tmpfile.Close()
	if err := packer.Packer.Finalize(); err != nil {
		return err
	}
	if err := packer.bufWr.Flush(); err != nil {
		return err
	}
	info, err := packer.tmpfile.Stat()
	if err != nil {
		return err
	}
	sink.mu.Lock()
	packBytes := uint64(info.Size())
	if sink.options.MaxAdditionalBytes > 0 && sink.reservedBytes+packBytes > sink.options.MaxAdditionalBytes {
		sink.mu.Unlock()
		return fmt.Errorf("deferred staging byte quota exceeded")
	}
	sink.reservedBytes += packBytes
	sink.mu.Unlock()
	reader, err := backend.NewFileReader(packer.tmpfile, nil)
	if err != nil {
		return err
	}
	hashReader := hashing.NewReader(reader, sha256.New())
	if _, err := io.Copy(io.Discard, hashReader); err != nil {
		return err
	}
	packID := vaultic.IDFromHash(hashReader.Sum(nil))
	packDigest := packID.String()
	handle := backend.Handle{Type: backend.PackFile, Name: packDigest, IsMetadata: blobType.IsMetadata()}
	placements := make([]staging.Placement, 0, len(sink.options.Backends))
	failures := make(map[string]error)
	domains := make(map[string]struct{})
	offsite := uint(0)
	for _, destination := range sink.options.Backends {
		if err := saveDeferredPack(ctx, destination.Backend, handle, packer.tmpfile, info.Size(), packDigest); err != nil {
			failures[destination.ID] = err
			continue
		}
		placements = append(placements, staging.Placement{BackendID: destination.ID, FailureDomain: destination.FailureDomain, Offsite: destination.Offsite, Size: info.Size(), SHA256: packDigest})
		domains[destination.FailureDomain] = struct{}{}
		if destination.Offsite {
			offsite++
		}
	}
	if uint(len(placements)) < sink.options.Policy.MinCopies || uint(len(domains)) < sink.options.Policy.MinDomains || offsite < sink.options.Policy.MinOffsite {
		return fmt.Errorf("deferred pack %s placement policy unsatisfied: %v", packDigest, failures)
	}
	payloadSize := uint64(0)
	for _, blob := range packer.Packer.Blobs() {
		payloadSize += uint64(blob.Length)
	}
	packFact := staging.Pack{ID: packDigest, Type: blobType.String(), Size: info.Size(), PayloadSize: payloadSize, HeaderSize: uint64(info.Size()) - payloadSize, BlobCount: uint64(len(packer.Packer.Blobs())), SHA256: packDigest, Placements: placements}
	records := make([]staging.Record, 0, len(packer.Packer.Blobs()))
	for _, blob := range packer.Packer.Blobs() {
		payload, err := json.Marshal(staging.BlobFact{ID: blob.ID.String(), Type: blob.Type.String(), PackID: packDigest, Offset: blob.Offset, Length: blob.Length, UncompressedLength: blob.UncompressedLength})
		if err != nil {
			return err
		}
		records = append(records, staging.Record{Kind: "blob-fact-v1", Payload: payload})
	}
	sink.mu.Lock()
	sink.result.Packs = append(sink.result.Packs, packFact)
	sink.result.Records = append(sink.result.Records, records...)
	sink.mu.Unlock()
	return nil
}

func saveDeferredPack(ctx context.Context, destination backend.Backend, handle backend.Handle, file io.ReadSeeker, size int64, digest string) error {
	if info, err := destination.Stat(ctx, handle); err == nil {
		if info.Size != size {
			return fmt.Errorf("immutable pack %s has conflicting size", handle.Name)
		}
		return verifyDeferredPack(ctx, destination, handle, size, digest)
	} else if !destination.IsNotExist(err) {
		return err
	}
	var backendHash []byte
	if destinationHasher := destination.Hasher(); destinationHasher != nil {
		hashInput, err := backend.NewFileReader(file, nil)
		if err != nil {
			return err
		}
		backendReader := hashing.NewReader(hashInput, destinationHasher)
		if _, err := io.Copy(io.Discard, backendReader); err != nil {
			return err
		}
		backendHash = backendReader.Sum(nil)
	}
	reader, err := backend.NewFileReader(file, backendHash)
	if err != nil {
		return err
	}
	if err := destination.Save(ctx, handle, reader); err != nil {
		return err
	}
	return verifyDeferredPack(ctx, destination, handle, size, digest)
}

func verifyDeferredPack(ctx context.Context, destination backend.Backend, handle backend.Handle, size int64, digest string) error {
	var stored []byte
	err := destination.Load(ctx, handle, int(size), 0, func(reader io.Reader) error {
		var err error
		stored, err = io.ReadAll(io.LimitReader(reader, size+1))
		return err
	})
	if err != nil {
		return err
	}
	actual := sha256.Sum256(stored)
	if int64(len(stored)) != size || !bytes.Equal(actual[:], mustDecodeID(digest)) {
		return fmt.Errorf("deferred pack %s verification failed", handle.Name)
	}
	return nil
}

func mustDecodeID(value string) []byte {
	id, err := vaultic.ParseID(value)
	if err != nil {
		return nil
	}
	return id[:]
}
