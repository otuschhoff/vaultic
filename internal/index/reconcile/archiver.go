package reconcile

import (
	"github.com/otuschhoff/vaultic/internal/archiver"
	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/fs"
)

// Attach configures an archiver to consult verified SlateDB state before
// reusing content and to enqueue each final node for reconciliation. The
// returned function must be called after Snapshot returns and before the
// backup is reported durable.
func Attach(target *archiver.Archiver, reconciler *Reconciler) func() error {
	previousReuse := target.ReuseNode
	previousObserve := target.ReconcileNode
	target.ReuseNode = func(snapshotPath, sourcePath string, info *fs.ExtendedFileInfo, previous *data.Node) bool {
		return previousReuse(snapshotPath, sourcePath, info, previous) && reconciler.CanReuse(snapshotPath, sourcePath, info, previous)
	}
	target.ReconcileNode = func(snapshotPath, sourcePath string, node *data.Node) {
		previousObserve(snapshotPath, sourcePath, node)
		reconciler.Observe(snapshotPath, sourcePath, node)
	}
	return reconciler.Close
}
