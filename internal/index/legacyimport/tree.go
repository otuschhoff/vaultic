package legacyimport

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"math"
	"path"

	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

type SnapshotSource interface {
	Source
	vaultic.BlobLoader
}

func importSnapshotMetadata(ctx context.Context, source Source, store Store, options Options) (result Result, err error) {
	result.SnapshotMetadataOnly = true
	publisher, ok := store.(interface {
		ImportLegacySnapshot(context.Context, schema.ID, schema.SnapshotRecord) error
	})
	if !ok {
		return result, fmt.Errorf("snapshot metadata import requires snapshot publication support")
	}
	legacy := legacySnapshotSource{source}
	snapshots, err := vaultic.MemorizeList(ctx, legacy, vaultic.SnapshotFile)
	if err != nil {
		return result, err
	}
	if err := snapshots.List(ctx, vaultic.SnapshotFile, func(vaultic.ID, int64) error {
		result.SnapshotsTotal++
		return nil
	}); err != nil {
		return result, err
	}
	report := func() {
		progress := Progress{SnapshotsTotal: result.SnapshotsTotal, SnapshotsCompleted: result.SnapshotsSeen,
			SnapshotsImported: result.SnapshotsImported, SnapshotsResumed: result.SnapshotsResumed}
		options.Telemetry.progress(progress)
		if options.Progress != nil {
			options.Progress(progress)
		}
	}
	report()
	err = data.ForAllSnapshots(ctx, snapshots, legacy, nil, func(id vaultic.ID, snapshot *data.Snapshot, loadErr error) error {
		result.SnapshotsSeen++
		defer report()
		if loadErr != nil {
			return recordFinding(&result, options, id, "decode-snapshot", loadErr)
		}
		if snapshot.Tree == nil || snapshot.Tree.IsNull() {
			return recordFinding(&result, options, id, "snapshot-root", fmt.Errorf("snapshot has no root tree"))
		}
		original, err := legacy.LoadUnpacked(ctx, vaultic.SnapshotFile, id)
		if err != nil {
			return err
		}
		record := schema.SnapshotRecord{LegacyTree: schema.ID(*snapshot.Tree), OriginalJSON: original}
		if _, err := record.MarshalBinary(); err != nil {
			return recordFinding(&result, options, id, "decode-snapshot", err)
		}
		rootValue, found, err := store.Get(ctx, schema.BlobKey(record.LegacyTree))
		if err != nil {
			return err
		}
		if !found {
			return recordFinding(&result, options, id, "snapshot-root", fmt.Errorf("root tree is absent from the blob catalog; import indexes first"))
		}
		root, err := schema.UnmarshalBlobRecord(rootValue)
		if err != nil {
			return err
		}
		hasTree := false
		for _, location := range root.Locations {
			hasTree = hasTree || location.Type == schema.BlobTree
		}
		if !hasTree {
			return recordFinding(&result, options, id, "snapshot-root", fmt.Errorf("root has no tree location"))
		}
		value, found, err := store.Get(ctx, schema.SnapshotKey(schema.ID(id)))
		if err != nil {
			return err
		}
		if found {
			existing, err := schema.UnmarshalSnapshotRecord(value)
			if err != nil {
				return err
			}
			if !bytes.Equal(existing.OriginalJSON, original) {
				return fmt.Errorf("snapshot %s conflicts with stored JSON", id.String())
			}
			if options.Resume {
				result.SnapshotsResumed++
				return nil
			}
		}
		if !options.DryRun {
			if err := publisher.ImportLegacySnapshot(ctx, schema.ID(id), record); err != nil {
				return err
			}
		}
		result.SnapshotsImported++
		return nil
	})
	return result, err
}

type legacySnapshotSource struct{ Source }

func (source legacySnapshotSource) List(ctx context.Context, kind vaultic.FileType, visit func(vaultic.ID, int64) error) error {
	if legacy, ok := source.Source.(interface {
		ListLegacy(context.Context, vaultic.FileType, func(vaultic.ID, int64) error) error
	}); ok {
		return legacy.ListLegacy(ctx, kind, visit)
	}
	return source.Source.List(ctx, kind, visit)
}

func (source legacySnapshotSource) LoadUnpacked(ctx context.Context, kind vaultic.FileType, id vaultic.ID) ([]byte, error) {
	if legacy, ok := source.Source.(interface {
		LoadLegacyUnpacked(context.Context, vaultic.FileType, vaultic.ID) ([]byte, error)
	}); ok {
		return legacy.LoadLegacyUnpacked(ctx, kind, id)
	}
	return source.Source.LoadUnpacked(ctx, kind, id)
}

type TreeStore interface {
	Store
	ImportLegacySnapshot(context.Context, schema.ID, schema.SnapshotRecord) error
	AllocateRevisionBlock(context.Context, uint64) (uint64, error)
	WriteMutableBatch(context.Context, []daemon.Mutation, [][]byte, bool) error
	PublishRevisionBatch(context.Context, []byte, []byte, []byte, uint64, []daemon.Mutation, [][]byte) error
	PublishRevisionBatchDeferred(context.Context, []byte, []byte, []byte, uint64, []daemon.Mutation, [][]byte) error
	PublishRevisionBatchesDeferred(context.Context, []daemon.RevisionPublication) error
	PublishContentManifest(context.Context, []schema.ID, []daemon.Mutation, [][]byte) (schema.ID, error)
	PublishContentManifestDeferred(context.Context, []schema.ID, []daemon.Mutation, [][]byte) (schema.ID, error)
}

const (
	snapshotRevisionBlockSize = 1024
	snapshotDebtBatchSize     = 256
	snapshotRevisionBatchSize = 256
	snapshotProgressNodes     = 1024
)

type nodeIdentity struct {
	fsid  uint32
	inode uint64
}

type treeImporter struct {
	ctx              context.Context
	source           SnapshotSource
	store            TreeStore
	options          Options
	result           *Result
	snapshot         vaultic.ID
	ancestors        map[vaultic.ID]struct{}
	pendingDebt      []daemon.Mutation
	pendingRevisions []daemon.RevisionPublication
	pendingCurrent   map[string]struct{}
	revisionNext     uint64
	revisionEnd      uint64
	reportProgress   func()
}

//nolint:gocognit // Existing domain flow is an explicit complexity exception; new code remains gated.
func importSnapshots(
	ctx context.Context,
	snapshots vaultic.Lister,
	source SnapshotSource,
	store TreeStore,
	options Options,
	result *Result,
	reportProgress func(),
) error {
	return data.ForAllSnapshots(ctx, snapshots, legacySnapshotSource{source}, nil, func(snapshotID vaultic.ID, snapshot *data.Snapshot, loadErr error) error {
		result.SnapshotsSeen++
		defer reportProgress()
		if loadErr != nil {
			if err := writeDebt(ctx, debtWrite{
				store: store, options: options, result: result, snapshot: snapshotID,
				pathHint: "snapshot", reason: schema.DebtMissingDirectory, errorClass: "snapshot-decode-failed",
			}); err != nil {
				return err
			}
			return recordFinding(result, options, snapshotID, "decode-snapshot", loadErr)
		}
		if snapshot.Tree == nil || snapshot.Tree.IsNull() {
			if err := writeDebt(ctx, debtWrite{
				store: store, options: options, result: result, snapshot: snapshotID,
				pathHint: "snapshot-root", reason: schema.DebtMissingDirectory, errorClass: "snapshot-root-missing",
			}); err != nil {
				return err
			}
			return recordFinding(result, options, snapshotID, "snapshot-root", fmt.Errorf("snapshot has no root tree"))
		}
		originalJSON, err := (legacySnapshotSource{source}).LoadUnpacked(ctx, vaultic.SnapshotFile, snapshotID)
		if err != nil {
			return err
		}
		record := schema.SnapshotRecord{LegacyTree: schema.ID(*snapshot.Tree), OriginalJSON: originalJSON}
		if _, err := record.MarshalBinary(); err != nil {
			return err
		}
		publish := func() error {
			if options.DryRun {
				return nil
			}
			return store.ImportLegacySnapshot(ctx, schema.ID(snapshotID), record)
		}
		checkpointKey := schema.SnapshotImportCheckpointKey(schema.ID(snapshotID))
		if options.Resume {
			value, found, err := store.Get(ctx, checkpointKey)
			if err != nil {
				return fmt.Errorf("read snapshot checkpoint for %s: %w", snapshotID.Str(), err)
			}
			if found {
				if _, err := schema.UnmarshalSnapshotImportCheckpointRecord(value); err != nil {
					return fmt.Errorf("decode snapshot checkpoint for %s: %w", snapshotID.Str(), err)
				}
				result.SnapshotsResumed++
				return publish()
			}
		}
		beforeTrees, beforeNodes, beforeDebts := result.TreesVisited, result.NodesImported, result.CrawlDebtCreated
		beforeErrors := result.ErrorsSeen
		importer := treeImporter{
			ctx:            ctx,
			source:         source,
			store:          store,
			options:        options,
			result:         result,
			snapshot:       snapshotID,
			ancestors:      make(map[vaultic.ID]struct{}),
			pendingCurrent: make(map[string]struct{}),
			reportProgress: reportProgress,
		}
		_, _, err = importer.importTree(*snapshot.Tree, nil, "", 0)
		if err != nil {
			return err
		}
		if err := importer.flushRevisions(); err != nil {
			return err
		}
		if err := importer.flushDebt(); err != nil {
			return err
		}
		if result.ErrorsSeen != beforeErrors {
			return nil
		}
		if err := publish(); err != nil {
			return err
		}
		if !options.DryRun {
			checkpoint := schema.SnapshotImportCheckpointRecord{
				TreesVisited: result.TreesVisited - beforeTrees, NodesImported: result.NodesImported - beforeNodes,
				DebtsCreated: result.CrawlDebtCreated - beforeDebts,
			}
			encoded, err := checkpoint.MarshalBinary()
			if err != nil {
				return err
			}
			if err := store.Put(ctx, checkpointKey, encoded, true); err != nil {
				return fmt.Errorf("publish snapshot checkpoint for %s: %w", snapshotID.Str(), err)
			}
		}
		result.SnapshotsImported++
		return nil
	})
}

func (importer *treeImporter) importTree(treeID vaultic.ID, parent *nodeIdentity, parentPath string, depth uint) ([]schema.DirectoryChild, bool, error) {
	if _, cycle := importer.ancestors[treeID]; cycle {
		if err := importer.writeDebt(debtWrite{
			store: importer.store, options: importer.options, result: importer.result,
			snapshot: importer.snapshot, work: treeID, pathHint: parentPath,
			reason: schema.DebtMissingDirectory, errorClass: "tree-cycle",
		}); err != nil {
			return nil, false, err
		}
		return nil, false, nil
	}
	importer.ancestors[treeID] = struct{}{}
	defer delete(importer.ancestors, treeID)
	tree, err := data.LoadTree(importer.ctx, importer.source, treeID)
	if err != nil {
		if debtErr := importer.writeDebt(debtWrite{
			store: importer.store, options: importer.options, result: importer.result,
			snapshot: importer.snapshot, work: treeID, pathHint: parentPath,
			reason: schema.DebtMissingDirectory, errorClass: "tree-load-failed",
		}); debtErr != nil {
			return nil, false, debtErr
		}
		if findingErr := recordFinding(importer.result, importer.options, importer.snapshot, "load-tree", err); findingErr != nil {
			return nil, false, findingErr
		}
		return nil, false, nil
	}
	importer.result.TreesVisited++
	children := make([]schema.DirectoryChild, 0)
	complete := true
	for item := range tree {
		if item.Error != nil {
			if findingErr := recordFinding(importer.result, importer.options, importer.snapshot, "decode-tree", item.Error); findingErr != nil {
				return nil, false, findingErr
			}
			return children, false, nil
		}
		if importer.options.SnapshotWorkBudget > 0 && importer.result.NodesVisited >= importer.options.SnapshotWorkBudget {
			return nil, false, ErrLimitReached
		}
		importer.result.NodesVisited++
		if importer.result.NodesVisited%snapshotProgressNodes == 0 {
			importer.reportProgress()
		}
		nodePath := path.Join(parentPath, item.Node.Name)
		child, imported, err := importer.importNode(item.Node, parent, nodePath, depth)
		if err != nil {
			return nil, false, err
		}
		if !imported {
			complete = false
			continue
		}
		children = append(children, child)
	}
	return children, complete, nil
}

//nolint:gocognit // Existing domain flow is an explicit complexity exception; new code remains gated.
func (importer *treeImporter) importNode(node *data.Node, parent *nodeIdentity, nodePath string, depth uint) (schema.DirectoryChild, bool, error) {
	identity, identityKnown := legacyIdentity(node)
	parentKnown := parent != nil
	if !identityKnown || !parentKnown {
		if err := importer.writeDebt(debtWrite{
			store: importer.store, options: importer.options, result: importer.result,
			snapshot: importer.snapshot, work: workID(nodePath), pathHint: nodePath,
			reason: schema.DebtMissingInode, errorClass: "legacy-node-identity-or-parent-unknown",
		}); err != nil {
			return schema.DirectoryChild{}, false, err
		}
	}
	if err := importer.writeDebt(debtWrite{
		store: importer.store, options: importer.options, result: importer.result,
		snapshot: importer.snapshot, work: workID(nodePath), pathHint: nodePath,
		reason: schema.DebtUnknownFreshness, errorClass: "legacy-metadata-not-live-verified",
	}); err != nil {
		return schema.DirectoryChild{}, false, err
	}
	nodeType := convertNodeType(node.Type)
	//nolint:nestif // Existing domain flow is an explicit complexity exception; new code remains gated.
	if node.Type == data.NodeTypeDir {
		if node.Subtree == nil || node.Subtree.IsNull() {
			if err := importer.writeDebt(debtWrite{
				store: importer.store, options: importer.options, result: importer.result,
				snapshot: importer.snapshot, work: workID(nodePath), pathHint: nodePath,
				reason: schema.DebtMissingDirectory, errorClass: "directory-subtree-missing",
			}); err != nil {
				return schema.DirectoryChild{}, false, err
			}
			return schema.DirectoryChild{}, false, nil
		}
		if importer.options.SnapshotDepth > 0 && depth >= importer.options.SnapshotDepth {
			if err := importer.writeDebt(debtWrite{
				store: importer.store, options: importer.options, result: importer.result,
				snapshot: importer.snapshot, work: *node.Subtree, pathHint: nodePath,
				reason: schema.DebtMissingDirectory, errorClass: "snapshot-depth-limit",
			}); err != nil {
				return schema.DirectoryChild{}, false, err
			}
			return schema.DirectoryChild{}, false, nil
		}
		var nextParent *nodeIdentity
		if identityKnown {
			nextParent = &identity
		}
		children, complete, err := importer.importTree(*node.Subtree, nextParent, nodePath, depth+1)
		if err != nil {
			return schema.DirectoryChild{}, false, err
		}
		if !identityKnown || !parentKnown || !complete {
			return schema.DirectoryChild{}, false, nil
		}
		record := schema.DirectoryRevision{
			ParentInode: parent.inode, Children: children, Size: node.Size, Mode: uint32(node.Mode), UID: node.UID, GID: node.GID,
			Known: schema.KnownParent | schema.KnownPath, SourcePath: nodePath, Freshness: schema.FreshnessImported,
		}
		if !node.ModTime.IsZero() {
			record.MTime = node.ModTime.UnixNano()
			record.Known |= schema.KnownMTime
		}
		if !node.ChangeTime.IsZero() {
			record.CTime = node.ChangeTime.UnixNano()
			record.Known |= schema.KnownCTime
		}
		if node.Mode != 0 {
			record.Known |= schema.KnownMode
		}
		if node.Size != 0 {
			record.Known |= schema.KnownSize
		}
		if node.UID != 0 {
			record.Known |= schema.KnownUID
		}
		if node.GID != 0 {
			record.Known |= schema.KnownGID
		}
		value, err := record.MarshalBinary()
		if err != nil {
			return schema.DirectoryChild{}, false, err
		}
		key, err := importer.publishRevision(schema.CurrentDirectoryKey(identity.fsid, identity.inode), identity, value, true, nil)
		if err != nil {
			return schema.DirectoryChild{}, false, err
		}
		return schema.DirectoryChild{Name: node.Name, Inode: identity.inode, Type: schema.NodeDirectory, MetadataKey: key}, true, nil
	}
	if !identityKnown || !parentKnown {
		return schema.DirectoryChild{}, false, nil
	}
	record, contentIDs, err := importer.inodeRecord(node, *parent, nodePath)
	if err != nil {
		return schema.DirectoryChild{}, false, err
	}
	value, err := record.MarshalBinary()
	if err != nil {
		return schema.DirectoryChild{}, false, err
	}
	key, err := importer.publishRevision(schema.CurrentInodeKey(identity.fsid, identity.inode), identity, value, false, contentIDs)
	if err != nil {
		return schema.DirectoryChild{}, false, err
	}
	return schema.DirectoryChild{Name: node.Name, Inode: identity.inode, Type: nodeType, MetadataKey: key}, true, nil
}

func (importer *treeImporter) inodeRecord(node *data.Node, parent nodeIdentity, nodePath string) (schema.InodeRevision, []schema.ID, error) {
	record := schema.InodeRevision{
		ParentInode: parent.inode, Size: node.Size,
		Mode: uint32(node.Mode), UID: node.UID, GID: node.GID, Known: schema.KnownParent | schema.KnownPath,
		SourcePath: nodePath, Freshness: schema.FreshnessImported,
	}
	if !node.ModTime.IsZero() {
		record.MTime = node.ModTime.UnixNano()
		record.Known |= schema.KnownMTime
	}
	if !node.ChangeTime.IsZero() {
		record.CTime = node.ChangeTime.UnixNano()
		record.Known |= schema.KnownCTime
	}
	if node.Size != 0 {
		record.Known |= schema.KnownSize
	}
	if node.Mode != 0 {
		record.Known |= schema.KnownMode
	}
	if node.UID != 0 {
		record.Known |= schema.KnownUID
	}
	if node.GID != 0 {
		record.Known |= schema.KnownGID
	}
	if node.Type != data.NodeTypeFile || len(node.Content) == 0 {
		record.ContentMode = schema.ContentNone
		return record, nil, nil
	}
	ids := make([]schema.ID, len(node.Content))
	for index, id := range node.Content {
		ids[index] = schema.ID(id)
	}
	record.ContentCount = uint32(len(ids))
	//nolint:nestif // Existing domain flow is an explicit complexity exception; new code remains gated.
	if len(ids) <= schema.MaxInlineContentIDs {
		record.ContentMode = schema.ContentInline
		record.ContentIDs = ids
	} else {
		reverse := make([]daemon.Mutation, 0, len(ids))
		manifestID := schema.ContentManifestID(ids)
		segments := make(map[schema.ID]uint32, len(ids))
		for index, id := range ids {
			if _, found := segments[id]; !found {
				segments[id] = uint32(index / schema.DefaultContentSegmentIDs)
			}
		}
		for id, segment := range segments {
			value, err := (schema.ReverseManifestRecord{Segment: segment, State: schema.ReferenceUnresolved}).MarshalBinary()
			if err != nil {
				return schema.InodeRevision{}, nil, err
			}
			reverse = append(reverse, daemon.Mutation{Key: schema.ReverseManifestKey(id, manifestID), Value: value})
		}
		if !importer.options.DryRun {
			publish := importer.store.PublishContentManifest
			if importer.options.DeferSnapshotDurability {
				publish = importer.store.PublishContentManifestDeferred
			}
			createdID, err := publish(importer.ctx, ids, reverse, nil)
			if err != nil {
				return schema.InodeRevision{}, nil, err
			}
			if createdID != manifestID {
				return schema.InodeRevision{}, nil, fmt.Errorf("content manifest identity mismatch")
			}
		}
		record.ContentMode = schema.ContentManifestRef
		record.ContentManifestID = manifestID
	}
	return record, ids, nil
}

func (importer *treeImporter) publishRevision(currentKey []byte, identity nodeIdentity, value []byte, directory bool, contentIDs []schema.ID) ([]byte, error) {
	if !importer.options.DeferSnapshotDurability {
		if existing, found, err := importer.store.Get(importer.ctx, currentKey); err != nil {
			return nil, err
		} else if found {
			pointer, err := schema.UnmarshalCurrentPointer(existing)
			if err != nil {
				return nil, err
			}
			existingValue, valueFound, err := importer.store.Get(importer.ctx, pointer.RecordKey)
			if err != nil {
				return nil, err
			}
			if valueFound && bytes.Equal(existingValue, value) {
				importer.result.NodesImported++
				return pointer.RecordKey, nil
			}
		}
	}
	if importer.options.DryRun {
		key := schema.InodeRevisionKey(identity.fsid, identity.inode, 1)
		if directory {
			key = schema.DirectoryRevisionKey(identity.fsid, identity.inode, 1)
		}
		importer.result.NodesImported++
		return key, nil
	}
	revision, err := importer.allocateRevision()
	if err != nil {
		return nil, err
	}
	revisionKey := schema.InodeRevisionKey(identity.fsid, identity.inode, revision)
	if directory {
		revisionKey = schema.DirectoryRevisionKey(identity.fsid, identity.inode, revision)
	}
	related := make([]daemon.Mutation, 0, len(contentIDs))
	for _, id := range uniqueIDs(contentIDs) {
		reverseValue, err := (schema.ReverseInodeRecord{LatestRevision: revision, State: schema.ReferenceUnresolved}).MarshalBinary()
		if err != nil {
			return nil, err
		}
		related = append(related, daemon.Mutation{Key: schema.ReverseInodeKey(id, identity.fsid, identity.inode), Value: reverseValue})
	}
	related = append(related, importer.pendingDebt...)
	if importer.options.DeferSnapshotDurability {
		currentID := string(currentKey)
		if _, duplicate := importer.pendingCurrent[currentID]; duplicate {
			if err := importer.flushRevisions(); err != nil {
				return nil, err
			}
		}
		importer.pendingRevisions = append(importer.pendingRevisions, daemon.RevisionPublication{
			CurrentKey: currentKey, RevisionKey: revisionKey, RevisionValue: value, Revision: revision,
			RelatedPuts: related,
		})
		importer.pendingCurrent[currentID] = struct{}{}
		importer.pendingDebt = importer.pendingDebt[:0]
		if len(importer.pendingRevisions) >= snapshotRevisionBatchSize {
			if err := importer.flushRevisions(); err != nil {
				return nil, err
			}
		}
	} else {
		if err := importer.store.PublishRevisionBatch(importer.ctx, currentKey, revisionKey, value, revision, related, nil); err != nil {
			return nil, err
		}
		importer.pendingDebt = importer.pendingDebt[:0]
	}
	importer.result.NodesImported++
	return revisionKey, nil
}

func (importer *treeImporter) flushRevisions() error {
	if len(importer.pendingRevisions) == 0 {
		return nil
	}
	if err := importer.store.PublishRevisionBatchesDeferred(importer.ctx, importer.pendingRevisions); err != nil {
		return err
	}
	importer.pendingRevisions = importer.pendingRevisions[:0]
	clear(importer.pendingCurrent)
	return nil
}

func (importer *treeImporter) allocateRevision() (uint64, error) {
	if importer.revisionNext == importer.revisionEnd {
		start, err := importer.store.AllocateRevisionBlock(importer.ctx, snapshotRevisionBlockSize)
		if err != nil {
			return 0, err
		}
		importer.revisionNext = start
		importer.revisionEnd = start + snapshotRevisionBlockSize
	}
	revision := importer.revisionNext
	importer.revisionNext++
	return revision, nil
}

func (importer *treeImporter) writeDebt(debt debtWrite) error {
	mutation, err := debtMutation(debt)
	if err != nil {
		return err
	}
	if !debt.options.DryRun {
		importer.pendingDebt = append(importer.pendingDebt, mutation)
		if len(importer.pendingDebt) >= snapshotDebtBatchSize {
			if err := importer.flushDebt(); err != nil {
				return err
			}
		}
	}
	debt.result.CrawlDebtCreated++
	return nil
}

func (importer *treeImporter) flushDebt() error {
	if len(importer.pendingDebt) == 0 {
		return nil
	}
	durable := !importer.options.DeferSnapshotDurability
	if err := importer.store.WriteMutableBatch(importer.ctx, importer.pendingDebt, nil, durable); err != nil {
		return err
	}
	importer.pendingDebt = importer.pendingDebt[:0]
	return nil
}

func uniqueIDs(ids []schema.ID) []schema.ID {
	result := make([]schema.ID, 0, len(ids))
	seen := make(map[schema.ID]struct{}, len(ids))
	for _, id := range ids {
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		result = append(result, id)
	}
	return result
}

func legacyIdentity(node *data.Node) (nodeIdentity, bool) {
	if node.Inode == 0 || node.DeviceID == 0 || node.DeviceID > math.MaxUint32 {
		return nodeIdentity{}, false
	}
	return nodeIdentity{fsid: uint32(node.DeviceID), inode: node.Inode}, true
}

func convertNodeType(nodeType data.NodeType) schema.NodeType {
	switch nodeType {
	case data.NodeTypeFile:
		return schema.NodeFile
	case data.NodeTypeDir:
		return schema.NodeDirectory
	case data.NodeTypeSymlink:
		return schema.NodeSymlink
	default:
		return schema.NodeOther
	}
}

type debtWrite struct {
	store      Store
	options    Options
	result     *Result
	snapshot   vaultic.ID
	work       vaultic.ID
	pathHint   string
	reason     schema.DebtReason
	errorClass string
}

func writeDebt(ctx context.Context, debt debtWrite) error {
	mutation, err := debtMutation(debt)
	if err != nil {
		return err
	}
	if !debt.options.DryRun {
		if err := debt.store.Put(ctx, mutation.Key, mutation.Value, true); err != nil {
			return err
		}
	}
	debt.result.CrawlDebtCreated++
	return nil
}

func debtMutation(debt debtWrite) (daemon.Mutation, error) {
	if debt.work.IsNull() {
		debt.work = workID(fmt.Sprintf("%d:%s", debt.reason, debt.pathHint))
	}
	key := schema.CrawlDebtKey(schema.ID(debt.snapshot), schema.ID(workID(fmt.Sprintf("%d:%s:%s", debt.reason, debt.work.String(), debt.pathHint))))
	record := schema.CrawlDebtRecord{
		SourceIndexOrPack: schema.ID(debt.snapshot), SourceKnown: !debt.snapshot.IsNull(), PathOrTree: []byte(debt.pathHint),
		Reason: debt.reason, Status: schema.DebtPending, ErrorClass: debt.errorClass,
	}
	encoded, err := record.MarshalBinary()
	if err != nil {
		return daemon.Mutation{}, err
	}
	return daemon.Mutation{Key: key, Value: encoded}, nil
}

func workID(value string) vaultic.ID {
	return vaultic.ID(sha256.Sum256([]byte(value)))
}
