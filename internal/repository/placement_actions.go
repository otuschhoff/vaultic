package repository

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/otuschhoff/vaultic/internal/backend"
	metadataindex "github.com/otuschhoff/vaultic/internal/index"
	"github.com/otuschhoff/vaultic/internal/index/schema"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

// PlacePack copies an encrypted pack object to an addressable placement. The
// operation is idempotent when the destination already has the expected size.
func (r *Repository) PlacePack(ctx context.Context, packID vaultic.ID, targetHash uint64) error {
	target, sources, handle, expectedSize, err := r.placementIO(ctx, packID, targetHash)
	if err != nil {
		return err
	}
	if info, statErr := target.Stat(ctx, handle); statErr == nil && uint64(info.Size) == expectedSize {
		return nil
	}
	file, err := os.CreateTemp("", "vaultic-placement-*")
	if err != nil {
		return err
	}
	name := file.Name()
	defer func() {
		_ = file.Close()    // Successful upload paths sync and read the file before this cleanup.
		_ = os.Remove(name) // The temporary placement file is never authoritative repository state.
	}()
	var loadErr error
	loaded := false
	var contentHash []byte
	for _, source := range sources {
		if source == target {
			continue
		}
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			return err
		}
		if err := file.Truncate(0); err != nil {
			return err
		}
		hasher := target.Hasher()
		loadErr = source.Load(ctx, handle, 0, 0, func(reader io.Reader) error {
			writer := io.Writer(file)
			if hasher != nil {
				writer = io.MultiWriter(file, hasher)
			}
			_, err := io.Copy(writer, reader)
			return err
		})
		if loadErr == nil {
			loaded = true
			if hasher != nil {
				contentHash = hasher.Sum(nil)
			}
			break
		}
	}
	if !loaded && loadErr == nil {
		return fmt.Errorf("placement %s has no independent source for repair", packID.Str())
	}
	if loadErr != nil {
		return fmt.Errorf("load source pack %s: %w", packID.Str(), loadErr)
	}
	reader, err := backend.NewFileReader(file, contentHash)
	if err != nil {
		return err
	}
	if err := target.Save(ctx, handle, reader); err != nil {
		return fmt.Errorf("save pack %s to placement: %w", packID.Str(), err)
	}
	return nil
}

// EvictPack removes one physical placement without touching the pack catalog.
// The scheduler must validate the post-removal durability predicate first.
func (r *Repository) EvictPack(ctx context.Context, packID vaultic.ID, targetHash uint64) error {
	target, _, handle, _, err := r.placementIO(ctx, packID, targetHash)
	if err != nil {
		return err
	}
	model, err := r.PlacementModel()
	if err != nil {
		return err
	}
	if target != r.be {
		if err := target.Remove(ctx, handle); err != nil && !target.IsNotExist(err) {
			return err
		}
		return nil
	}
	if !model.HotCold {
		return fmt.Errorf("cannot evict the only physical repository backend")
	}
	if err := target.Remove(ctx, handle); err != nil && !target.IsNotExist(err) {
		return err
	}
	return nil
}

func (r *Repository) placementIO(
	ctx context.Context,
	packID vaultic.ID,
	targetHash uint64,
) (backend.Backend, []backend.Backend, backend.Handle, uint64, error) {
	engine, ok := r.Engine().(*metadataindex.DaemonEngine)
	if !ok {
		return nil, nil, backend.Handle{}, 0, fmt.Errorf("placement I/O requires the SlateDB daemon engine")
	}
	value, found, err := engine.SchemaStore().Get(ctx, schema.PackKey(schema.ID(packID)))
	if err != nil {
		return nil, nil, backend.Handle{}, 0, err
	}
	if !found {
		return nil, nil, backend.Handle{}, 0, fmt.Errorf("pack %s is not cataloged", packID.Str())
	}
	record, err := schema.UnmarshalPackRecord(value)
	if err != nil {
		return nil, nil, backend.Handle{}, 0, err
	}
	model, err := r.PlacementModel()
	if err != nil {
		return nil, nil, backend.Handle{}, 0, err
	}
	registered, found := model.BackendByIDHash(targetHash)
	if !found {
		return nil, nil, backend.Handle{}, 0, fmt.Errorf("placement backend %016x is not registered", targetHash)
	}
	handle := backend.Handle{Type: backend.PackFile, Name: packID.String(), IsMetadata: record.Type == schema.PackTree}
	target, found := r.backendForPlacement(model, registered)
	if !found {
		return nil, nil, backend.Handle{}, 0, fmt.Errorf("placement backend %q is not addressable by hot/cold topology", registered.ID)
	}
	sources := make([]backend.Backend, 0, len(model.Backends))
	seen := make(map[backend.Backend]struct{})
	for _, candidate := range model.Backends {
		physical, ok := r.backendForPlacement(model, candidate)
		if !ok {
			continue
		}
		if _, duplicate := seen[physical]; duplicate {
			continue
		}
		seen[physical] = struct{}{}
		sources = append(sources, physical)
	}
	return target, sources, handle, record.PhysicalSize, nil
}

func (r *Repository) backendForPlacement(model PlacementModel, placement PlacementBackend) (backend.Backend, bool) {
	if attached, found := r.placementBackend(placement.Hash); found {
		return attached, true
	}
	hot, cold, hotCold := r.HotCold()
	if hotCold {
		switch placement.Role {
		case PlacementRolePrimary:
			return hot, true
		case PlacementRoleArchival:
			return cold, true
		}
	}
	if placement.Role == PlacementRolePrimary && placement.Location == "" {
		return r.be, true
	}
	if len(model.Backends) == 1 {
		return r.be, true
	}
	return nil, false
}
