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

type TreeStore interface {
	Store
	AllocateRevision(context.Context) (uint64, error)
	PublishRevisionBatch(context.Context, []byte, []byte, []byte, uint64, []daemon.Mutation, [][]byte) error
	PublishContentManifest(context.Context, []schema.ID, []daemon.Mutation, [][]byte) (schema.ID, error)
}

type nodeIdentity struct {
	fsid  uint32
	inode uint64
}

type treeImporter struct {
	ctx       context.Context
	source    SnapshotSource
	store     TreeStore
	options   Options
	result    *Result
	snapshot  vaultic.ID
	ancestors map[vaultic.ID]struct{}
}

func importSnapshots(ctx context.Context, source SnapshotSource, store TreeStore, options Options, result *Result) error {
	return data.ForAllSnapshots(ctx, source, source, nil, func(snapshotID vaultic.ID, snapshot *data.Snapshot, loadErr error) error {
		result.SnapshotsSeen++
		if loadErr != nil {
			if err := writeDebt(ctx, store, options, result, snapshotID, vaultic.ID{}, "snapshot", schema.DebtMissingDirectory, "snapshot-decode-failed"); err != nil {
				return err
			}
			return recordFinding(result, options, snapshotID, "decode-snapshot", loadErr)
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
				return nil
			}
		}
		if snapshot.Tree == nil || snapshot.Tree.IsNull() {
			if err := writeDebt(ctx, store, options, result, snapshotID, vaultic.ID{}, "snapshot-root", schema.DebtMissingDirectory, "snapshot-root-missing"); err != nil {
				return err
			}
			return recordFinding(result, options, snapshotID, "snapshot-root", fmt.Errorf("snapshot has no root tree"))
		}
		beforeTrees, beforeNodes, beforeDebts := result.TreesVisited, result.NodesImported, result.CrawlDebtCreated
		importer := treeImporter{ctx: ctx, source: source, store: store, options: options, result: result, snapshot: snapshotID, ancestors: make(map[vaultic.ID]struct{})}
		_, _, err := importer.importTree(*snapshot.Tree, nil, "", 0)
		if err != nil {
			return err
		}
		if err := writeDebt(ctx, store, options, result, snapshotID, *snapshot.Tree, "snapshot-root", schema.DebtMissingInode, "legacy-snapshot-root-has-no-inode-identity"); err != nil {
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
		if err := writeDebt(importer.ctx, importer.store, importer.options, importer.result, importer.snapshot, treeID, parentPath, schema.DebtMissingDirectory, "tree-cycle"); err != nil {
			return nil, false, err
		}
		return nil, false, nil
	}
	importer.ancestors[treeID] = struct{}{}
	defer delete(importer.ancestors, treeID)
	tree, err := data.LoadTree(importer.ctx, importer.source, treeID)
	if err != nil {
		if debtErr := writeDebt(importer.ctx, importer.store, importer.options, importer.result, importer.snapshot, treeID, parentPath, schema.DebtMissingDirectory, "tree-load-failed"); debtErr != nil {
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

func (importer *treeImporter) importNode(node *data.Node, parent *nodeIdentity, nodePath string, depth uint) (schema.DirectoryChild, bool, error) {
	identity, identityKnown := legacyIdentity(node)
	parentKnown := parent != nil
	if !identityKnown || !parentKnown {
		if err := writeDebt(importer.ctx, importer.store, importer.options, importer.result, importer.snapshot, workID(nodePath), nodePath, schema.DebtMissingInode, "legacy-node-identity-or-parent-unknown"); err != nil {
			return schema.DirectoryChild{}, false, err
		}
	}
	if err := writeDebt(importer.ctx, importer.store, importer.options, importer.result, importer.snapshot, workID(nodePath), nodePath, schema.DebtUnknownFreshness, "legacy-metadata-not-live-verified"); err != nil {
		return schema.DirectoryChild{}, false, err
	}
	nodeType := convertNodeType(node.Type)
	if node.Type == data.NodeTypeDir {
		if node.Subtree == nil || node.Subtree.IsNull() {
			if err := writeDebt(importer.ctx, importer.store, importer.options, importer.result, importer.snapshot, workID(nodePath), nodePath, schema.DebtMissingDirectory, "directory-subtree-missing"); err != nil {
				return schema.DirectoryChild{}, false, err
			}
			return schema.DirectoryChild{}, false, nil
		}
		if importer.options.SnapshotDepth > 0 && depth >= importer.options.SnapshotDepth {
			if err := writeDebt(importer.ctx, importer.store, importer.options, importer.result, importer.snapshot, *node.Subtree, nodePath, schema.DebtMissingDirectory, "snapshot-depth-limit"); err != nil {
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
		record := schema.DirectoryRevision{ParentInode: parent.inode, Children: children}
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
			createdID, err := importer.store.PublishContentManifest(importer.ctx, ids, reverse, nil)
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
	if importer.options.DryRun {
		key := schema.InodeRevisionKey(identity.fsid, identity.inode, 1)
		if directory {
			key = schema.DirectoryRevisionKey(identity.fsid, identity.inode, 1)
		}
		importer.result.NodesImported++
		return key, nil
	}
	revision, err := importer.store.AllocateRevision(importer.ctx)
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
	if err := importer.store.PublishRevisionBatch(importer.ctx, currentKey, revisionKey, value, revision, related, nil); err != nil {
		return nil, err
	}
	importer.result.NodesImported++
	return revisionKey, nil
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

func writeDebt(ctx context.Context, store Store, options Options, result *Result, snapshot, work vaultic.ID, pathHint string, reason schema.DebtReason, errorClass string) error {
	if work.IsNull() {
		work = workID(fmt.Sprintf("%d:%s", reason, pathHint))
	}
	key := schema.CrawlDebtKey(schema.ID(snapshot), schema.ID(workID(fmt.Sprintf("%d:%s:%s", reason, work.String(), pathHint))))
	record := schema.CrawlDebtRecord{
		SourceIndexOrPack: schema.ID(snapshot), SourceKnown: !snapshot.IsNull(), PathOrTree: []byte(pathHint),
		Reason: reason, Status: schema.DebtPending, ErrorClass: errorClass,
	}
	encoded, err := record.MarshalBinary()
	if err != nil {
		return err
	}
	if !options.DryRun {
		if err := store.Put(ctx, key, encoded, true); err != nil {
			return err
		}
	}
	result.CrawlDebtCreated++
	return nil
}

func workID(value string) vaultic.ID {
	return vaultic.ID(sha256.Sum256([]byte(value)))
}
