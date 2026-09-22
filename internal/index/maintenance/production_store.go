package maintenance

import (
	"context"

	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
	monitor "github.com/otuschhoff/vaultic/internal/telemetry"
)

type productionStore struct {
	Store
	accounting *monitor.ProductionAccounting
}

func withProductionStore(store Store) Store {
	if _, wrapped := store.(*productionStore); wrapped {
		return store
	}
	return &productionStore{Store: store, accounting: monitor.DefaultProductionAccounting()}
}

func (store *productionStore) Get(ctx context.Context, key []byte) (value []byte, found bool, resultErr error) {
	dependency := store.accounting.StartDependency(ctx, "database")
	defer func() { dependency.Finish(resultErr) }()
	return store.Store.Get(ctx, key)
}

func (store *productionStore) MultiGet(ctx context.Context, keys [][]byte) (values []daemon.KeyValue, found []bool, resultErr error) {
	dependency := store.accounting.StartDependency(ctx, "database")
	defer func() { dependency.Finish(resultErr) }()
	return store.Store.MultiGet(ctx, keys)
}

func (store *productionStore) ScanPrefix(ctx context.Context, prefix, after []byte, limit uint32) (values []daemon.KeyValue, more bool, resultErr error) {
	dependency := store.accounting.StartDependency(ctx, "database")
	defer func() { dependency.Finish(resultErr) }()
	return store.Store.ScanPrefix(ctx, prefix, after, limit)
}

func (store *productionStore) ScanRange(ctx context.Context, prefix []byte, limit uint32, consume func([]daemon.KeyValue) error) error {
	dependency := store.accounting.StartDependency(ctx, "database")
	err := scanRange(ctx, store.Store, prefix, limit, func(entries []daemon.KeyValue) error {
		dependency.Finish(nil)
		err := consume(entries)
		dependency = store.accounting.StartDependency(ctx, "database")
		return err
	})
	dependency.Finish(err)
	return err
}

func (store *productionStore) MarkIndexPublished(ctx context.Context, id schema.ID, packs []schema.ID) (sequence uint64, resultErr error) {
	dependency := store.accounting.StartDependency(ctx, "database")
	defer func() { dependency.Finish(resultErr) }()
	return store.Store.MarkIndexPublished(ctx, id, packs)
}

func (store *productionStore) WriteMutableBatch(ctx context.Context, puts []daemon.Mutation, deletes [][]byte, deferDurability bool) (resultErr error) {
	dependency := store.accounting.StartDependency(ctx, "database")
	defer func() { dependency.Finish(resultErr) }()
	return store.Store.WriteMutableBatch(ctx, puts, deletes, deferDurability)
}

func unwrapProductionStore(store Store) Store {
	if wrapped, ok := store.(*productionStore); ok {
		return wrapped.Store
	}
	return store
}
