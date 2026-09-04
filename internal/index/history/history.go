package history

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strings"

	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

const SchemaVersion = 1
const scanPageSize = 10_000

type Store interface {
	Get(context.Context, []byte) ([]byte, bool, error)
	MultiGet(context.Context, [][]byte) ([]daemon.KeyValue, []bool, error)
	ScanPrefix(context.Context, []byte, []byte, uint32) ([]daemon.KeyValue, bool, error)
}

type Options struct {
	SinceCommit uint64
	UntilCommit uint64
	Snapshots   bool
	Content     bool
}

type Binding struct {
	Covered  bool
	Present  bool
	Inode    uint64
	Revision uint64
	NodeType schema.NodeType
}

type SnapshotPoint struct {
	SnapshotID string `json:"snapshot_id"`
	Commit     uint64 `json:"commit"`
	Time       int64  `json:"time,omitempty"`
}

type PathAtResult struct {
	SchemaVersion  int      `json:"schema_version"`
	Path           string   `json:"path"`
	SnapshotID     string   `json:"snapshot_id"`
	Commit         uint64   `json:"commit"`
	Covered        bool     `json:"covered"`
	Present        bool     `json:"present"`
	Inode          uint64   `json:"inode,omitempty"`
	Revision       uint64   `json:"revision,omitempty"`
	NodeType       string   `json:"node_type,omitempty"`
	Freshness      string   `json:"freshness,omitempty"`
	MTime          int64    `json:"mtime,omitempty"`
	CTime          int64    `json:"ctime,omitempty"`
	Size           uint64   `json:"size,omitempty"`
	MetadataKnown  []string `json:"metadata_known,omitempty"`
	Hardlinks      []string `json:"hardlinks,omitempty"`
	DirectoryChain []string `json:"directory_chain,omitempty"`
}

type Change struct {
	Kind           string          `json:"kind"`
	Path           string          `json:"path"`
	Commit         uint64          `json:"commit"`
	SnapshotID     string          `json:"snapshot_id"`
	Covered        bool            `json:"covered"`
	Present        bool            `json:"present"`
	Inode          uint64          `json:"inode,omitempty"`
	Revision       uint64          `json:"revision,omitempty"`
	NodeType       string          `json:"node_type,omitempty"`
	ContentID      string          `json:"content_id,omitempty"`
	ContentChanged bool            `json:"content_changed,omitempty"`
	MetadataOnly   bool            `json:"metadata_only,omitempty"`
	Snapshots      []SnapshotPoint `json:"snapshots,omitempty"`
}

type Metrics struct {
	SnapshotsScanned      uint64  `json:"snapshots_scanned"`
	BindingChanges        uint64  `json:"binding_changes"`
	PathComponents        uint64  `json:"path_components"`
	DirectoryLookups      uint64  `json:"directory_lookups"`
	DirectoryCacheHits    uint64  `json:"directory_cache_hits"`
	MultiGetBatches       uint64  `json:"multi_get_batches"`
	AveragePathComponents float64 `json:"average_path_components"`
}

type FileHistoryResult struct {
	SchemaVersion int      `json:"schema_version"`
	Path          string   `json:"path"`
	Source        string   `json:"source"`
	Changes       []Change `json:"changes"`
	Metrics       Metrics  `json:"metrics"`
}

type InodeRevisionEntry struct {
	FSID          uint32   `json:"fsid"`
	Inode         uint64   `json:"inode"`
	Revision      uint64   `json:"revision"`
	Freshness     string   `json:"freshness"`
	MTime         int64    `json:"mtime,omitempty"`
	CTime         int64    `json:"ctime,omitempty"`
	Size          uint64   `json:"size,omitempty"`
	MetadataKnown []string `json:"metadata_known,omitempty"`
}

type InodeHistoryResult struct {
	SchemaVersion int                  `json:"schema_version"`
	FSID          uint32               `json:"fsid"`
	Inode         uint64               `json:"inode"`
	Revisions     []InodeRevisionEntry `json:"revisions"`
}

func InodeHistory(
	ctx context.Context,
	store Store,
	fsid uint32,
	inode uint64,
	since, until uint64,
) (InodeHistoryResult, error) {
	result := InodeHistoryResult{SchemaVersion: SchemaVersion, FSID: fsid, Inode: inode}
	err := scan(ctx, store, schema.InodeRevisionPrefix(fsid, inode), func(kv daemon.KeyValue) error {
		parsed, err := schema.ParseKey(kv.Key)
		if err != nil || parsed.Kind != schema.KeyInodeRevision {
			return fmt.Errorf("invalid inode revision key %q", kv.Key)
		}
		if since != 0 && parsed.Revision < since {
			return nil
		}
		if until != 0 && parsed.Revision >= until {
			return nil
		}
		record, err := schema.UnmarshalInodeRevision(kv.Value)
		if err != nil {
			return err
		}
		result.Revisions = append(result.Revisions, InodeRevisionEntry{
			FSID: fsid, Inode: inode, Revision: parsed.Revision,
			Freshness: freshnessName(record.Freshness), MTime: record.MTime,
			CTime: record.CTime, Size: record.Size, MetadataKnown: knownFields(record.Known),
		})
		return nil
	})
	return result, err
}

type snapshotEntry struct {
	ID      schema.ID
	Commit  uint64
	Time    int64
	RootKey []byte
	Paths   []string
}

type resolvedPath struct {
	snapshot  snapshotEntry
	covered   bool
	present   bool
	inode     uint64
	revision  uint64
	nodeType  schema.NodeType
	key       []byte
	chain     [][]byte
	inodeRec  *schema.InodeRevision
	dirRec    *schema.DirectoryRevision
	hardlinks []string
}

func PathAt(ctx context.Context, store Store, targetPath, snapshotID string) (PathAtResult, error) {
	targetPath = cleanTargetPath(targetPath)
	snapshots, err := loadSnapshots(ctx, store, 0, 0)
	if err != nil {
		return PathAtResult{}, err
	}
	for _, snapshot := range snapshots {
		if vaultic.ID(snapshot.ID).String() != snapshotID {
			continue
		}
		resolved, err := newResolver(store).resolve(ctx, snapshot, targetPath)
		if err != nil {
			return PathAtResult{}, err
		}
		return resultFromResolved(targetPath, resolved), nil
	}
	return PathAtResult{}, fmt.Errorf("snapshot %q is not present in the snapshot commit index", snapshotID)
}

func ResolvePathAtCommit(ctx context.Context, store Store, targetPath string, commit uint64) (Binding, error) {
	snapshots, err := loadSnapshots(ctx, store, commit, commit+1)
	if err != nil {
		return Binding{}, err
	}
	if len(snapshots) == 0 {
		return Binding{}, fmt.Errorf("commit %d is not present in the snapshot commit index", commit)
	}
	resolved, err := newResolver(store).resolve(ctx, snapshots[0], cleanTargetPath(targetPath))
	if err != nil {
		return Binding{}, err
	}
	return Binding{
		Covered:  resolved.covered,
		Present:  resolved.present,
		Inode:    resolved.inode,
		Revision: resolved.revision,
		NodeType: resolved.nodeType,
	}, nil
}

func FileHistory(ctx context.Context, store Store, targetPath string, options Options) (FileHistoryResult, error) {
	targetPath = cleanTargetPath(targetPath)
	snapshots, err := loadSnapshots(ctx, store, options.SinceCommit, options.UntilCommit)
	if err != nil {
		return FileHistoryResult{}, err
	}
	resolver := newResolver(store)
	resolved := make([]resolvedPath, 0, len(snapshots))
	for _, snapshot := range snapshots {
		entry, err := resolver.resolve(ctx, snapshot, targetPath)
		if err != nil {
			return FileHistoryResult{}, err
		}
		resolved = append(resolved, entry)
	}
	changes := coalesceChanges(targetPath, resolved, options.Snapshots, options.Content)
	metrics := resolver.metrics
	metrics.SnapshotsScanned = uint64(len(snapshots))
	metrics.BindingChanges = uint64(len(changes))
	if metrics.SnapshotsScanned != 0 {
		metrics.AveragePathComponents = float64(metrics.PathComponents) / float64(metrics.SnapshotsScanned)
	}
	return FileHistoryResult{
		SchemaVersion: SchemaVersion,
		Path:          targetPath,
		Source:        "walk",
		Changes:       changes,
		Metrics:       metrics,
	}, nil
}

func FileHistoryFromPathIndex(
	ctx context.Context,
	store Store,
	targetPath string,
	options Options,
) (FileHistoryResult, bool, error) {
	targetPath = cleanTargetPath(targetPath)
	changes := make([]Change, 0)
	err := scan(ctx, store, schema.PathVersionPrefix(0, targetPath), func(kv daemon.KeyValue) error {
		parsed, err := schema.ParseKey(kv.Key)
		if err != nil || parsed.Kind != schema.KeyPathVersion {
			return fmt.Errorf("invalid path-version key %q", kv.Key)
		}
		if options.SinceCommit != 0 && parsed.Revision < options.SinceCommit {
			return nil
		}
		if options.UntilCommit != 0 && parsed.Revision >= options.UntilCommit {
			return nil
		}
		record, err := schema.UnmarshalPathVersionRecord(kv.Value)
		if err != nil {
			return err
		}
		binding, err := ResolvePathAtCommit(ctx, store, targetPath, parsed.Revision)
		if err != nil {
			return err
		}
		present := record.State == schema.PathBound && binding.Present && binding.Inode == record.Inode &&
			binding.Revision == record.Revision
		change := Change{Path: targetPath, Commit: parsed.Revision, Covered: binding.Covered, Present: present}
		if record.State == schema.PathOverflow {
			change.Kind = "overflow"
		} else if !binding.Covered {
			change.Kind = "not-covered"
		} else if record.State == schema.PathTombstone {
			change.Kind = "deleted"
		} else {
			change.Kind = "bound"
			change.Inode, change.Revision, change.NodeType = record.Inode, record.Revision, nodeTypeName(record.NodeType)
		}
		changes = append(changes, change)
		return nil
	})
	if err != nil || len(changes) == 0 {
		return FileHistoryResult{}, false, err
	}
	return FileHistoryResult{
		SchemaVersion: SchemaVersion,
		Path:          targetPath,
		Source:        "path-index",
		Changes:       changes,
		Metrics:       Metrics{BindingChanges: uint64(len(changes))},
	}, true, nil
}

func loadSnapshots(ctx context.Context, store Store, since, until uint64) ([]snapshotEntry, error) {
	entries := make([]snapshotEntry, 0)
	err := scan(ctx, store, schema.SnapshotCommitPrefix(), func(kv daemon.KeyValue) error {
		parsed, err := schema.ParseKey(kv.Key)
		if err != nil || parsed.Kind != schema.KeySnapshotCommit {
			return fmt.Errorf("invalid snapshot commit key %q", kv.Key)
		}
		if since != 0 && parsed.Revision < since {
			return nil
		}
		if until != 0 && parsed.Revision >= until {
			return nil
		}
		record, err := schema.UnmarshalSnapshotCommitRecord(kv.Value)
		if err != nil {
			return err
		}
		snapshotValue, found, err := store.Get(ctx, schema.SnapshotKey(parsed.ID))
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf(
				"snapshot commit %d references missing snapshot %s",
				parsed.Revision,
				vaultic.ID(parsed.ID).String(),
			)
		}
		snapshot, err := schema.UnmarshalSnapshotRecord(snapshotValue)
		if err != nil {
			return err
		}
		entries = append(
			entries,
			snapshotEntry{
				ID:      parsed.ID,
				Commit:  parsed.Revision,
				Time:    record.SnapshotTimeUnixNano,
				RootKey: append([]byte(nil), record.RootKey...),
				Paths:   snapshotScopePaths(snapshot.OriginalJSON),
			},
		)
		return nil
	})
	return entries, err
}

type resolver struct {
	store   Store
	cache   map[string]resolveMemo
	metrics Metrics
}

type resolveMemo struct {
	present  bool
	inode    uint64
	revision uint64
	nodeType schema.NodeType
	key      []byte
	chain    [][]byte
}

func newResolver(store Store) *resolver {
	return &resolver{store: store, cache: map[string]resolveMemo{}}
}

func (r *resolver) resolve(ctx context.Context, snapshot snapshotEntry, targetPath string) (resolvedPath, error) {
	parts := pathParts(targetPath)
	result := resolvedPath{snapshot: snapshot, covered: pathCovered(snapshot.Paths, targetPath)}
	if len(parts) == 0 {
		parsed, err := schema.ParseKey(snapshot.RootKey)
		if err != nil || parsed.Kind != schema.KeyDirectoryRevision {
			return result, fmt.Errorf("snapshot %s has invalid root key", vaultic.ID(snapshot.ID).String())
		}
		result.present,
			result.inode,
			result.revision,
			result.nodeType,
			result.key,
			result.chain = true,
			parsed.Inode,
			parsed.Revision,
			schema.NodeDirectory,
			snapshot.RootKey,
			[][]byte{
				append([]byte(nil), snapshot.RootKey...),
			}
		return result, nil
	}
	memo, err := r.resolveFromDirectory(ctx, snapshot.RootKey, parts)
	if err != nil {
		return result, err
	}
	result.present,
		result.inode,
		result.revision,
		result.nodeType,
		result.key,
		result.chain = memo.present,
		memo.inode,
		memo.revision,
		memo.nodeType,
		memo.key,
		memo.chain
	if !result.present {
		return result, nil
	}
	value, found, err := r.store.Get(ctx, result.key)
	if err != nil {
		return result, err
	}
	if !found {
		return result, fmt.Errorf("resolved revision %x is missing", result.key)
	}
	if result.nodeType == schema.NodeDirectory {
		dir, err := schema.UnmarshalDirectoryRevision(value)
		if err != nil {
			return result, err
		}
		result.dirRec = &dir
		return result, nil
	}
	inode, err := schema.UnmarshalInodeRevision(value)
	if err != nil {
		return result, err
	}
	result.inodeRec = &inode
	if inode.HasMultipleParents {
		result.hardlinks, err = r.loadHardlinks(ctx, result.key)
		if err != nil {
			return result, err
		}
	}
	return result, nil
}

func (r *resolver) loadHardlinks(ctx context.Context, inodeKey []byte) ([]string, error) {
	parsed, err := schema.ParseKey(inodeKey)
	if err != nil || parsed.Kind != schema.KeyInodeRevision {
		return nil, fmt.Errorf("hardlink target is not an inode revision")
	}
	value, found, err := r.store.Get(ctx, schema.HardlinkRefsKey(parsed.FSID, parsed.Inode, parsed.Revision))
	if err != nil || !found {
		return nil, err
	}
	record, err := schema.UnmarshalHardlinkRefsRecord(value)
	if err != nil {
		return nil, err
	}
	refs := make([]string, 0, len(record.Parents))
	for _, parent := range record.Parents {
		refs = append(refs, fmt.Sprintf("%d/%s", parent.ParentInode, parent.Name))
	}
	return refs, nil
}

func (r *resolver) resolveFromDirectory(ctx context.Context, directoryKey []byte, parts []string) (resolveMemo, error) {
	if len(parts) == 0 {
		parsed, err := schema.ParseKey(directoryKey)
		if err != nil || parsed.Kind != schema.KeyDirectoryRevision {
			return resolveMemo{}, fmt.Errorf("invalid directory key %x", directoryKey)
		}
		return resolveMemo{
			present:  true,
			inode:    parsed.Inode,
			revision: parsed.Revision,
			nodeType: schema.NodeDirectory,
			key:      append([]byte(nil), directoryKey...),
		}, nil
	}
	cacheKey := string(directoryKey) + "\x00" + strings.Join(parts, "/")
	if memo, ok := r.cache[cacheKey]; ok {
		r.metrics.DirectoryCacheHits++
		return memo, nil
	}
	r.metrics.DirectoryLookups++
	r.metrics.PathComponents++
	values, foundSet, err := r.store.MultiGet(ctx, [][]byte{directoryKey})
	r.metrics.MultiGetBatches++
	if err != nil {
		return resolveMemo{}, err
	}
	if len(foundSet) != 1 || !foundSet[0] || len(values) != 1 {
		return resolveMemo{}, fmt.Errorf("directory revision %x is missing", directoryKey)
	}
	directory, err := schema.UnmarshalDirectoryRevision(values[0].Value)
	if err != nil {
		return resolveMemo{}, err
	}
	index := sort.Search(len(directory.Children), func(i int) bool { return directory.Children[i].Name >= parts[0] })
	if index >= len(directory.Children) || directory.Children[index].Name != parts[0] {
		memo := resolveMemo{}
		r.cache[cacheKey] = memo
		return memo, nil
	}
	child := directory.Children[index]
	parsed, err := schema.ParseKey(child.MetadataKey)
	if err != nil {
		return resolveMemo{}, err
	}
	if len(parts) == 1 {
		memo := resolveMemo{
			present:  true,
			inode:    parsed.Inode,
			revision: parsed.Revision,
			nodeType: child.Type,
			key:      append([]byte(nil), child.MetadataKey...),
			chain:    [][]byte{append([]byte(nil), directoryKey...)},
		}
		r.cache[cacheKey] = memo
		return memo, nil
	}
	if child.Type != schema.NodeDirectory || parsed.Kind != schema.KeyDirectoryRevision {
		memo := resolveMemo{}
		r.cache[cacheKey] = memo
		return memo, nil
	}
	memo, err := r.resolveFromDirectory(ctx, child.MetadataKey, parts[1:])
	if err != nil {
		return resolveMemo{}, err
	}
	memo.chain = append([][]byte{append([]byte(nil), directoryKey...)}, memo.chain...)
	r.cache[cacheKey] = memo
	return memo, nil
}

func resultFromResolved(targetPath string, resolved resolvedPath) PathAtResult {
	result := PathAtResult{
		SchemaVersion: SchemaVersion,
		Path:          targetPath,
		SnapshotID:    vaultic.ID(resolved.snapshot.ID).String(),
		Commit:        resolved.snapshot.Commit,
		Covered:       resolved.covered,
		Present:       resolved.present,
	}
	if !resolved.present {
		return result
	}
	result.Inode, result.Revision, result.NodeType = resolved.inode, resolved.revision, nodeTypeName(resolved.nodeType)
	if resolved.inodeRec != nil {
		result.Freshness = freshnessName(resolved.inodeRec.Freshness)
		result.MTime = resolved.inodeRec.MTime
		result.CTime = resolved.inodeRec.CTime
		result.Size = resolved.inodeRec.Size
		result.MetadataKnown = knownFields(resolved.inodeRec.Known)
		result.Hardlinks = resolved.hardlinks
	}
	for _, key := range resolved.chain {
		result.DirectoryChain = append(result.DirectoryChain, fmt.Sprintf("%x", key))
	}
	return result
}

func coalesceChanges(targetPath string, resolved []resolvedPath, annotate, includeContent bool) []Change {
	changes := make([]Change, 0)
	var previous resolvedPath
	for index, current := range resolved {
		kind := "created"
		if index > 0 {
			switch {
			case !previous.covered && !current.covered:
				previous = current
				continue
			case previous.present && current.present && previous.inode == current.inode && previous.revision == current.revision:
				if annotate && len(changes) != 0 {
					last := &changes[len(changes)-1]
					if last.Present && last.Inode == current.inode && last.Revision == current.revision {
						last.Snapshots = append(last.Snapshots, snapshotPoint(current.snapshot))
					}
				}
				previous = current
				continue
			case previous.present && current.present && previous.inode == current.inode:
				kind = "modified"
			case previous.present && current.present && previous.inode != current.inode:
				kind = "rebound"
			case previous.present && !current.present:
				if !current.covered {
					kind = "not-covered"
				} else {
					kind = "deleted"
				}
			case !previous.present && current.present:
				kind = "created"
			case !previous.present && !current.present:
				previous = current
				continue
			}
		} else if !current.present && !current.covered {
			kind = "not-covered"
		} else if !current.present {
			previous = current
			continue
		}
		change := Change{
			Kind:       kind,
			Path:       targetPath,
			Commit:     current.snapshot.Commit,
			SnapshotID: vaultic.ID(current.snapshot.ID).String(),
			Covered:    current.covered,
			Present:    current.present,
			Inode:      current.inode,
			Revision:   current.revision,
			NodeType:   nodeTypeName(current.nodeType),
		}
		if includeContent && current.present && current.inodeRec != nil {
			currentContent := contentIdentity(current.inodeRec)
			change.ContentID = currentContent
			if index > 0 && previous.present && previous.inode == current.inode && previous.inodeRec != nil {
				previousContent := contentIdentity(previous.inodeRec)
				change.ContentChanged = previousContent != currentContent
				change.MetadataOnly = previousContent == currentContent && previous.revision != current.revision
			}
		}
		if annotate {
			change.Snapshots = []SnapshotPoint{snapshotPoint(current.snapshot)}
		}
		changes = append(changes, change)
		previous = current
	}
	return changes
}

func contentIdentity(record *schema.InodeRevision) string {
	if record == nil {
		return "unknown"
	}
	if record.HashKnown {
		return vaultic.ID(record.FileContentHash).String()
	}
	switch record.ContentMode {
	case schema.ContentInline:
		parts := make([]string, 0, len(record.ContentIDs))
		for _, id := range record.ContentIDs {
			parts = append(parts, vaultic.ID(id).String())
		}
		return "inline:" + strings.Join(parts, ",")
	case schema.ContentManifestRef:
		return "manifest:" + vaultic.ID(record.ContentManifestID).String()
	case schema.ContentNone:
		return "none"
	default:
		return "unknown"
	}
}

func snapshotPoint(snapshot snapshotEntry) SnapshotPoint {
	return SnapshotPoint{SnapshotID: vaultic.ID(snapshot.ID).String(), Commit: snapshot.Commit, Time: snapshot.Time}
}

func scan(ctx context.Context, store Store, prefix []byte, visit func(daemon.KeyValue) error) error {
	var after []byte
	for {
		entries, done, err := store.ScanPrefix(ctx, prefix, after, scanPageSize)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if err := visit(entry); err != nil {
				return err
			}
			after = append(after[:0], entry.Key...)
		}
		if done {
			return nil
		}
		if len(entries) == 0 {
			return fmt.Errorf("scan %q made no progress", prefix)
		}
	}
}

func cleanTargetPath(value string) string {
	cleaned := path.Clean("/" + strings.TrimSpace(value))
	if cleaned == "/" {
		return "/"
	}
	return strings.TrimPrefix(cleaned, "/")
}

func pathParts(value string) []string {
	value = cleanTargetPath(value)
	if value == "/" {
		return nil
	}
	return strings.Split(value, "/")
}

func snapshotScopePaths(original []byte) []string {
	var decoded struct {
		Paths []string `json:"paths"`
	}
	if json.Unmarshal(original, &decoded) != nil {
		return nil
	}
	return decoded.Paths
}

func pathCovered(scopes []string, targetPath string) bool {
	if len(scopes) == 0 {
		return true
	}
	targetPath = cleanTargetPath(targetPath)
	for _, scope := range scopes {
		cleaned := cleanTargetPath(scope)
		if cleaned == "/" || targetPath == cleaned || strings.HasPrefix(targetPath, cleaned+"/") {
			return true
		}
	}
	return false
}

func nodeTypeName(value schema.NodeType) string {
	switch value {
	case schema.NodeFile:
		return "file"
	case schema.NodeDirectory:
		return "directory"
	case schema.NodeSymlink:
		return "symlink"
	case schema.NodeOther:
		return "other"
	default:
		return "unknown"
	}
}

func freshnessName(value schema.Freshness) string {
	switch value {
	case schema.FreshnessVerified:
		return "verified"
	case schema.FreshnessImported:
		return "imported"
	default:
		return "unknown"
	}
}

func knownFields(mask uint16) []string {
	fields := []struct {
		bit  uint16
		name string
	}{
		{schema.KnownMTime, "mtime"}, {schema.KnownCTime, "ctime"}, {schema.KnownSize, "size"},
		{schema.KnownMode, "mode"}, {schema.KnownUID, "uid"}, {schema.KnownGID, "gid"},
		{schema.KnownParent, "parent"}, {schema.KnownPath, "path"},
	}
	result := make([]string, 0)
	for _, field := range fields {
		if mask&field.bit != 0 {
			result = append(result, field.name)
		}
	}
	return result
}
