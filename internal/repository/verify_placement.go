package repository

import (
	"bufio"
	"context"
	stderrors "errors"
	"fmt"

	"github.com/klauspost/compress/zstd"
	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/index/schema"
	"github.com/otuschhoff/vaultic/internal/repository/pack"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

type PlacementTarget struct {
	PackID  vaultic.ID
	Backend uint64
}

type PlacementVerificationError struct {
	Classification schema.VerificationClassification
	Stage          string
	Expected       string
	Observed       string
	Err            error
}

func (err *PlacementVerificationError) Error() string {
	return fmt.Sprintf("verify placement at %s: %v", err.Stage, err.Err)
}
func (err *PlacementVerificationError) Unwrap() error { return err.Err }
func (err *PlacementVerificationError) VerificationClassification() (schema.VerificationClassification, string, string) {
	return err.Classification, err.Expected, err.Observed
}

func (r *Repository) VerifyPackPlacement(ctx context.Context, packID vaultic.ID, backendHash uint64, level schema.VerificationLevel) error {
	target, _, handle, expectedSize, err := r.placementIO(ctx, packID, backendHash)
	if err != nil {
		return &PlacementVerificationError{Classification: schema.VerificationTransport, Stage: "resolve", Err: err}
	}
	info, err := target.Stat(ctx, handle)
	if err != nil {
		classification := schema.VerificationTransport
		if target.IsNotExist(err) {
			classification = schema.VerificationMissing
		}
		return &PlacementVerificationError{Classification: classification, Stage: "stat", Expected: fmt.Sprint(expectedSize), Err: err}
	}
	if uint64(info.Size) != expectedSize {
		return &PlacementVerificationError{Classification: schema.VerificationSizeMismatch, Stage: "size", Expected: fmt.Sprint(expectedSize), Observed: fmt.Sprint(info.Size), Err: fmt.Errorf("unexpected object size")}
	}
	blobs, _, err := pack.List(r.Key(), backend.ReaderAt(ctx, target, handle), info.Size)
	if err != nil {
		return &PlacementVerificationError{Classification: schema.VerificationHeaderAuthentication, Stage: "header", Err: err}
	}
	if level == schema.VerificationHeader {
		return nil
	}
	buffer, err := loadRaw(ctx, target, handle)
	if err != nil {
		return &PlacementVerificationError{Classification: schema.VerificationTransport, Stage: "read", Err: err}
	}
	observed := vaultic.Hash(buffer)
	if observed != packID {
		return &PlacementVerificationError{Classification: schema.VerificationChecksumMismatch, Stage: "checksum", Expected: packID.String(), Observed: observed.String(), Err: vaultic.ErrInvalidData}
	}
	if level == schema.VerificationChecksum {
		return nil
	}
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return err
	}
	defer decoder.Close()
	if err := checkPackInnerBackend(ctx, r, target, packID, pack.Blobs(blobs), info.Size, bufio.NewReaderSize(nil, maxStreamBufferSize), decoder); err != nil {
		classification, stage := schema.VerificationPayloadDecrypt, "payload"
		var blobError *blobVerificationError
		if stderrors.As(err, &blobError) && blobError.stage == "decompress" {
			classification, stage = schema.VerificationDecompression, "decompression"
		}
		return &PlacementVerificationError{Classification: classification, Stage: stage, Err: err}
	}
	return nil
}

func (r *Repository) WarmupPackPlacements(ctx context.Context, placements []PlacementTarget) error {
	type targetHandles struct {
		backend backend.Backend
		handles []backend.Handle
	}
	groups := make(map[backend.Backend]*targetHandles)
	for _, placement := range placements {
		target, _, handle, _, err := r.placementIO(ctx, placement.PackID, placement.Backend)
		if err != nil {
			return err
		}
		group := groups[target]
		if group == nil {
			group = &targetHandles{backend: target}
			groups[target] = group
		}
		group.handles = append(group.handles, handle)
	}
	for _, group := range groups {
		warming, err := group.backend.Warmup(ctx, group.handles)
		if err != nil {
			return err
		}
		if err := group.backend.WarmupWait(ctx, warming); err != nil {
			return err
		}
	}
	return nil
}
