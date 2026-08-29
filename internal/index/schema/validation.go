package schema

import (
	"fmt"
	"math"
	"sort"
)

func ClassifyPack(types []BlobType) PackType {
	if len(types) == 0 {
		return PackUnknown
	}
	hasData, hasTree := false, false
	for _, blobType := range types {
		switch blobType {
		case BlobData:
			hasData = true
		case BlobTree:
			hasTree = true
		default:
			return PackUnknown
		}
	}
	if hasData && hasTree {
		return PackMixed
	}
	if hasData {
		return PackData
	}
	return PackTree
}

func RebuildPackAggregates(records []PackRecord, updateSequence uint64) (map[AggregateKind]PackAggregate, error) {
	result := map[AggregateKind]PackAggregate{}
	for _, record := range records {
		record = record.normalized()
		if !validPackType(record.Type) || !validPackLifecycle(record.Lifecycle) {
			return nil, fmt.Errorf("%w: invalid pack record", ErrMalformed)
		}
		kind := map[PackType]AggregateKind{PackData: AggregateData, PackTree: AggregateTree, PackMixed: AggregateMixed, PackUnknown: AggregateUnknown}[record.Type]
		for _, aggregateKind := range []AggregateKind{kind, AggregateAll} {
			aggregate := result[aggregateKind]
			if err := accumulatePackAggregate(&aggregate, record); err != nil {
				return nil, err
			}
			aggregate.UpdateSequence = updateSequence
			result[aggregateKind] = aggregate
		}
	}
	for _, kind := range []AggregateKind{AggregateData, AggregateTree, AggregateMixed, AggregateUnknown, AggregateAll} {
		aggregate := result[kind]
		aggregate.UpdateSequence = updateSequence
		result[kind] = aggregate
	}
	return result, nil
}

// RebuildTierAggregates recomputes the tier dimension from pack records. Every
// tier is present in the result, including tiers with no packs, so a rebuild
// can overwrite a stale record rather than leaving it behind.
func RebuildTierAggregates(records []PackRecord, updateSequence uint64) (map[PackTier]PackAggregate, error) {
	result := map[PackTier]PackAggregate{}
	for _, record := range records {
		record = record.normalized()
		if !validPackType(record.Type) || !validPackLifecycle(record.Lifecycle) {
			return nil, fmt.Errorf("%w: invalid pack record", ErrMalformed)
		}
		if !validPackTier(record.Tier) {
			return nil, fmt.Errorf("%w: invalid pack tier", ErrMalformed)
		}
		aggregate := result[record.Tier]
		if err := accumulatePackAggregate(&aggregate, record); err != nil {
			return nil, err
		}
		aggregate.UpdateSequence = updateSequence
		result[record.Tier] = aggregate
	}
	for _, tier := range TierAggregateKinds() {
		aggregate := result[tier]
		aggregate.UpdateSequence = updateSequence
		result[tier] = aggregate
	}
	return result, nil
}

// accumulatePackAggregate adds one pack's totals to an aggregate. Usage bytes
// are only accumulated for packs whose usage is known, so an unaccounted pack
// never contributes zero used bytes as though it were wholly unreachable.
func accumulatePackAggregate(aggregate *PackAggregate, record PackRecord) error {
	var ok bool
	if aggregate.PackCount, ok = add(aggregate.PackCount, 1); !ok {
		return fmt.Errorf("%w: aggregate overflow", ErrMalformed)
	}
	if aggregate.PhysicalSize, ok = add(aggregate.PhysicalSize, record.PhysicalSize); !ok {
		return fmt.Errorf("%w: aggregate overflow", ErrMalformed)
	}
	if aggregate.PayloadSize, ok = add(aggregate.PayloadSize, record.PayloadSize); !ok {
		return fmt.Errorf("%w: aggregate overflow", ErrMalformed)
	}
	if aggregate.HeaderSize, ok = add(aggregate.HeaderSize, record.HeaderSize); !ok {
		return fmt.Errorf("%w: aggregate overflow", ErrMalformed)
	}
	if aggregate.BlobCount, ok = add(aggregate.BlobCount, record.BlobCount); !ok {
		return fmt.Errorf("%w: aggregate overflow", ErrMalformed)
	}
	if !record.UsageKnown {
		return nil
	}
	if aggregate.UsedPayloadBytes, ok = add(aggregate.UsedPayloadBytes, record.UsedPayloadBytes); !ok {
		return fmt.Errorf("%w: aggregate overflow", ErrMalformed)
	}
	if aggregate.UnusedPayloadBytes, ok = add(aggregate.UnusedPayloadBytes, record.UnusedPayloadBytes); !ok {
		return fmt.Errorf("%w: aggregate overflow", ErrMalformed)
	}
	if aggregate.AccountedPackCount, ok = add(aggregate.AccountedPackCount, 1); !ok {
		return fmt.Errorf("%w: aggregate overflow", ErrMalformed)
	}
	return nil
}

func add(left, right uint64) (uint64, bool) {
	if math.MaxUint64-left < right {
		return 0, false
	}
	return left + right, true
}

type InodeRef struct {
	FSID  uint32
	Inode uint64
}
type DirectoryNode struct {
	Revision uint64
	Record   DirectoryRevision
}

// ResolveSnapshotRoot resolves a snapshot through its immutable root revision,
// never through a mutable current-directory pointer.
func ResolveSnapshotRoot(snapshot SnapshotRecord, revisions map[string][]byte) (DirectoryRevision, error) {
	key := DirectoryRevisionKey(snapshot.RootFSID, snapshot.RootInode, snapshot.RootRevision)
	encoded, ok := revisions[string(key)]
	if !ok {
		return DirectoryRevision{}, fmt.Errorf("%w: snapshot root revision is missing", ErrMalformed)
	}
	return UnmarshalDirectoryRevision(encoded)
}

func ValidateDirectoryGraph(root InodeRef, directories map[InodeRef]DirectoryNode) error {
	rootNode, ok := directories[root]
	if !ok {
		return fmt.Errorf("%w: root directory is missing", ErrMalformed)
	}
	if rootNode.Record.ParentInode != 0 {
		return fmt.Errorf("%w: root parent must be zero", ErrMalformed)
	}
	parents := make(map[InodeRef]InodeRef)
	adjacency := make(map[InodeRef][]InodeRef)
	for parent, node := range directories {
		if node.Revision == 0 {
			return fmt.Errorf("%w: directory revision is zero", ErrMalformed)
		}
		for _, child := range node.Record.Children {
			if child.Type != NodeDirectory {
				continue
			}
			parsed, err := ParseKey(child.MetadataKey)
			if err != nil || parsed.Kind != KeyDirectoryRevision || parsed.Revision == 0 {
				return fmt.Errorf("%w: invalid directory child reference", ErrMalformed)
			}
			childRef := InodeRef{FSID: parsed.FSID, Inode: child.Inode}
			if childRef.FSID != parent.FSID {
				return fmt.Errorf("%w: directory child crosses filesystem boundary", ErrMalformed)
			}
			childNode, exists := directories[childRef]
			if !exists || childNode.Revision != parsed.Revision {
				return fmt.Errorf("%w: referenced directory revision is missing", ErrMalformed)
			}
			if childNode.Record.ParentInode != parent.Inode {
				return fmt.Errorf("%w: directory parent mismatch", ErrMalformed)
			}
			if existing, exists := parents[childRef]; exists && existing != parent {
				return fmt.Errorf("%w: conflicting directory parents", ErrMalformed)
			}
			parents[childRef] = parent
			adjacency[parent] = append(adjacency[parent], childRef)
		}
	}
	color := make(map[InodeRef]byte)
	var visit func(InodeRef) error
	visit = func(node InodeRef) error {
		if color[node] == 1 {
			return fmt.Errorf("%w: directory cycle", ErrMalformed)
		}
		if color[node] == 2 {
			return nil
		}
		color[node] = 1
		for _, child := range adjacency[node] {
			if err := visit(child); err != nil {
				return err
			}
		}
		color[node] = 2
		return nil
	}
	if err := visit(root); err != nil {
		return err
	}
	if len(color) != len(directories) {
		return fmt.Errorf("%w: directory graph contains unreachable nodes", ErrMalformed)
	}
	return nil
}

func AssembleContent(manifestID ID, segments []ContentManifest) ([]ID, error) {
	if len(segments) == 0 {
		return nil, fmt.Errorf("%w: content manifest has no segments", ErrMalformed)
	}
	sort.Slice(segments, func(i, j int) bool { return segments[i].Segment < segments[j].Segment })
	total, segmentCount := segments[0].TotalCount, segments[0].SegmentCount
	if total > MaxContentIDs || int(segmentCount) != len(segments) {
		return nil, fmt.Errorf("%w: missing content manifest segment", ErrMalformed)
	}
	actualCount := uint64(0)
	for _, segment := range segments {
		actualCount += uint64(len(segment.ContentIDs))
	}
	if actualCount != uint64(total) {
		return nil, fmt.Errorf("%w: content manifest count mismatch", ErrMalformed)
	}
	ids := make([]ID, 0, total)
	for index, segment := range segments {
		if segment.Segment != uint32(index) || segment.SegmentCount != segmentCount || segment.TotalCount != total {
			return nil, fmt.Errorf("%w: inconsistent content manifest segment", ErrMalformed)
		}
		ids = append(ids, segment.ContentIDs...)
	}
	if uint32(len(ids)) != total || ContentManifestID(ids) != manifestID {
		return nil, fmt.Errorf("%w: content manifest identity mismatch", ErrMalformed)
	}
	return ids, nil
}

type ManifestEdge struct {
	Blob     ID
	Manifest ID
}
type InodeEdge struct {
	Blob            ID
	FSID            uint32
	Inode, Revision uint64
}

func RebuildReferenceCounts(manifestEdges []ManifestEdge, inodeEdges []InodeEdge, updateSequence uint64) map[ID]ReferenceCountRecord {
	manifestSets := map[ID]map[ID]struct{}{}
	inodeSets := map[ID]map[InodeRef]struct{}{}
	revisionSets := map[ID]map[[20]byte]struct{}{}
	totals := map[ID]uint64{}
	for _, edge := range manifestEdges {
		if manifestSets[edge.Blob] == nil {
			manifestSets[edge.Blob] = map[ID]struct{}{}
		}
		manifestSets[edge.Blob][edge.Manifest] = struct{}{}
		totals[edge.Blob]++
	}
	for _, edge := range inodeEdges {
		if inodeSets[edge.Blob] == nil {
			inodeSets[edge.Blob] = map[InodeRef]struct{}{}
			revisionSets[edge.Blob] = map[[20]byte]struct{}{}
		}
		inodeSets[edge.Blob][InodeRef{edge.FSID, edge.Inode}] = struct{}{}
		var revision [20]byte
		copy(revision[:], InodeRevisionKey(edge.FSID, edge.Inode, edge.Revision)[3:])
		revisionSets[edge.Blob][revision] = struct{}{}
		totals[edge.Blob]++
	}
	result := map[ID]ReferenceCountRecord{}
	for blob, total := range totals {
		result[blob] = ReferenceCountRecord{TotalReferences: total, DistinctInodes: uint64(len(inodeSets[blob])), DistinctRevisions: uint64(len(revisionSets[blob])), DistinctManifests: uint64(len(manifestSets[blob])), UpdateSequence: updateSequence}
	}
	return result
}
