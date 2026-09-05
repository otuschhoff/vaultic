package repository

import (
	"encoding/json"
	"fmt"
	"sort"

	indexpkg "github.com/otuschhoff/vaultic/internal/index"
	legacyindex "github.com/otuschhoff/vaultic/internal/repository/index"
	"github.com/otuschhoff/vaultic/internal/repository/pack"
	"github.com/otuschhoff/vaultic/internal/repository/staging"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

// UseDeferredJournalIndex installs an in-memory index derived from an authenticated
// journal. It never publishes or mutates authoritative repository metadata.
//
//nolint:gocognit // Existing domain flow is an explicit complexity exception; new code remains gated.
func (r *Repository) UseDeferredJournalIndex(segments []staging.Segment) error {
	declared := make(map[string]staging.Pack)
	blobs := make(map[string]pack.Blobs)
	for _, segment := range segments {
		for _, stagedPack := range segment.Packs {
			if previous, ok := declared[stagedPack.ID]; ok &&
				(previous.Size != stagedPack.Size || previous.SHA256 != stagedPack.SHA256 || previous.BlobCount != stagedPack.BlobCount) {
				return fmt.Errorf("conflicting staged pack facts for %s", stagedPack.ID)
			}
			declared[stagedPack.ID] = stagedPack
		}
	}
	for _, segment := range segments {
		for _, record := range segment.Records {
			if record.Kind != "blob-fact-v1" {
				continue
			}
			var fact staging.BlobFact
			if err := json.Unmarshal(record.Payload, &fact); err != nil {
				return fmt.Errorf("decode staged blob fact: %w", err)
			}
			blobID, err := vaultic.ParseID(fact.ID)
			if err != nil {
				return fmt.Errorf("invalid staged blob ID: %w", err)
			}
			stagedPack, ok := declared[fact.PackID]
			if !ok {
				return fmt.Errorf("staged blob references undeclared pack %s", fact.PackID)
			}
			var blobType vaultic.BlobType
			switch fact.Type {
			case "data":
				blobType = vaultic.DataBlob
			case "tree":
				blobType = vaultic.TreeBlob
			default:
				return fmt.Errorf("invalid staged blob type %q", fact.Type)
			}
			if stagedPack.Type != fact.Type || fact.Length == 0 || uint64(fact.Offset)+uint64(fact.Length) > stagedPack.PayloadSize {
				return fmt.Errorf("invalid staged blob layout in pack %s", fact.PackID)
			}
			blobs[fact.PackID] = append(
				blobs[fact.PackID],
				pack.Blob{
					BlobHandle:         vaultic.BlobHandle{Type: blobType, ID: blobID},
					Offset:             fact.Offset,
					Length:             fact.Length,
					UncompressedLength: fact.UncompressedLength,
				},
			)
		}
	}
	master := legacyindex.NewMasterIndex()
	idx := legacyindex.NewIndex()
	for packIDString, stagedPack := range declared {
		packID, err := vaultic.ParseID(packIDString)
		if err != nil {
			return fmt.Errorf("invalid staged pack ID: %w", err)
		}
		packBlobs := blobs[packIDString]
		if uint64(len(packBlobs)) != stagedPack.BlobCount {
			return fmt.Errorf("staged pack %s blob count mismatch", packIDString)
		}
		sort.Slice(packBlobs, func(i, j int) bool { return packBlobs[i].Offset < packBlobs[j].Offset })
		var payloadBytes uint64
		var previousEnd uint64
		for index, blob := range packBlobs {
			start, end := uint64(blob.Offset), uint64(blob.Offset)+uint64(blob.Length)
			if index > 0 && start < previousEnd {
				return fmt.Errorf("staged pack %s has overlapping blob ranges", packIDString)
			}
			payloadBytes += uint64(blob.Length)
			previousEnd = end
		}
		if payloadBytes != stagedPack.PayloadSize {
			return fmt.Errorf("staged pack %s payload size mismatch", packIDString)
		}
		idx.StorePack(packID, packBlobs)
	}
	master.Insert(idx)
	r.idx = master
	r.SetEngine(indexpkg.NewRecoveryLegacyEngine(master))
	return nil
}
