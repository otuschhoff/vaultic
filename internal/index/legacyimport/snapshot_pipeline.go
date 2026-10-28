package legacyimport

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/workingstate"
)

type snapshotDebtWriter struct {
	ctx       context.Context
	cancel    context.CancelCauseFunc
	jobs      chan []daemon.Mutation
	available chan []daemon.Mutation
	done      chan struct{}
	finish    sync.Once
	release   func()
}

func (importer *treeImporter) startDebtWriter() error {
	if importer.debtBatchItems != snapshotDebtAdmittedItems || importer.debtWriter != nil {
		return fmt.Errorf("snapshot debt overlap requires an admitted batch and a fresh writer")
	}
	release, err := workingstate.Reserve(importer.ctx, 2*snapshotDebtWorkingCharge)
	if err != nil {
		return fmt.Errorf("reserve snapshot debt overlap buffers: %w", err)
	}
	ctx, cancel := context.WithCancelCause(importer.ctx)
	writer := &snapshotDebtWriter{
		ctx: ctx, cancel: cancel, jobs: make(chan []daemon.Mutation, 1),
		available: make(chan []daemon.Mutation, 3), done: make(chan struct{}),
		release: sync.OnceFunc(release),
	}
	for range 2 {
		writer.available <- make([]daemon.Mutation, 0, snapshotDebtAdmittedItems)
	}
	importer.debtWriter = writer
	go func() {
		defer close(writer.done)
		for {
			select {
			case <-ctx.Done():
				return
			case batch, open := <-writer.jobs:
				if !open {
					return
				}
				started := time.Now()
				err := importer.store.WriteMutableBatch(ctx, batch, nil, !importer.options.DeferSnapshotDurability)
				importer.options.Telemetry.observe("snapshot_debt_write", time.Since(started))
				clear(batch)
				writer.available <- batch[:0]
				if err != nil {
					cancel(err)
					return
				}
			}
		}
	}()
	return nil
}

func (writer *snapshotDebtWriter) submit(batch []daemon.Mutation) ([]daemon.Mutation, error) {
	select {
	case <-writer.ctx.Done():
		return nil, context.Cause(writer.ctx)
	case writer.jobs <- batch:
	}
	select {
	case <-writer.ctx.Done():
		return nil, context.Cause(writer.ctx)
	case next := <-writer.available:
		return next, nil
	}
}

func (importer *treeImporter) finishDebtWriter() error {
	if importer.debtWriter == nil {
		return nil
	}
	writer := importer.debtWriter
	started := time.Now()
	writer.finish.Do(func() { close(writer.jobs) })
	<-writer.done
	importer.options.Telemetry.observe("snapshot_debt_join", time.Since(started))
	return context.Cause(writer.ctx)
}

func (importer *treeImporter) stopDebtWriter() {
	if importer.debtWriter == nil {
		return
	}
	writer := importer.debtWriter
	writer.cancel(context.Canceled)
	writer.finish.Do(func() { close(writer.jobs) })
	<-writer.done
	for len(writer.jobs) > 0 {
		clear(<-writer.jobs)
	}
	for len(writer.available) > 0 {
		clear(<-writer.available)
	}
	clear(importer.pendingDebt)
	importer.pendingDebt = nil
	importer.debtWriter = nil
	writer.release()
}
