package reconcile

import (
	"fmt"
	"path"
	"sort"
	"strings"

	"github.com/otuschhoff/vaultic/internal/index/schema"
)

func (reconciler *Reconciler) planSnapshotAncestors(published map[string]publishedItem) (map[string][]schema.DirectoryChild, error) {
	bySnapshot := make(map[string]schema.NodeType, len(published))
	for _, item := range published {
		name := normalizeSnapshotPath(item.snapshotPath)
		if name == ".." || strings.HasPrefix(name, "../") {
			return nil, fmt.Errorf("snapshot path %q escapes the root", item.snapshotPath)
		}
		bySnapshot[name] = item.typeID
	}
	missing := make(map[string][]schema.DirectoryChild)
	for _, item := range published {
		if err := reconciler.ctx.Err(); err != nil {
			return nil, err
		}
		itemPath := normalizeSnapshotPath(item.snapshotPath)
		parent := path.Dir(itemPath)
		if parent == "." || itemPath == "" {
			continue
		}
		if ancestor, found := bySnapshot[parent]; found {
			if ancestor != schema.NodeDirectory {
				return nil, fmt.Errorf("snapshot ancestor %q is not a directory", parent)
			}
			continue
		}
		missing[parent] = append(missing[parent], schema.DirectoryChild{
			Name: path.Base(itemPath), Inode: item.identity.inode, Type: item.typeID, MetadataKey: item.key,
		})
		for ancestor := path.Dir(parent); ancestor != "."; ancestor = path.Dir(ancestor) {
			if _, exists := bySnapshot[ancestor]; exists {
				return nil, fmt.Errorf("snapshot has an unobserved directory below observed ancestor %q", ancestor)
			}
			if _, exists := missing[ancestor]; !exists {
				missing[ancestor] = nil
			}
		}
	}
	return missing, nil
}

func (reconciler *Reconciler) publishSnapshotAncestors(published map[string]publishedItem) ([]schema.DirectoryChild, error) {
	missing, err := reconciler.planSnapshotAncestors(published)
	if err != nil {
		return nil, err
	}
	paths := make([]string, 0, len(missing))
	for name := range missing {
		paths = append(paths, name)
	}
	sort.Slice(paths, func(left, right int) bool {
		return len(paths[left]) > len(paths[right]) || len(paths[left]) == len(paths[right]) && paths[left] < paths[right]
	})
	var roots []schema.DirectoryChild
	for _, name := range paths {
		children := missing[name]
		sort.Slice(children, func(left, right int) bool { return children[left].Name < children[right].Name })
		record := schema.DirectoryRevision{
			Children: children, SourcePath: name, Known: schema.KnownPath, Freshness: schema.FreshnessVerified,
		}
		value, err := record.MarshalBinary()
		if err != nil {
			return nil, err
		}
		revision, err := reconciler.store.AllocateRevision(reconciler.ctx)
		if err != nil {
			return nil, err
		}
		key := schema.DirectoryRevisionKey(0, revision, revision)
		if err := reconciler.store.PublishRevisionBatch(reconciler.ctx, schema.CurrentDirectoryKey(0, revision), key, value, revision, nil, nil); err != nil {
			return nil, err
		}
		child := schema.DirectoryChild{Name: path.Base(name), Inode: revision, Type: schema.NodeDirectory, MetadataKey: key}
		parent := path.Dir(name)
		if parent != "." {
			missing[parent] = append(missing[parent], child)
		} else {
			roots = append(roots, child)
		}
	}
	return roots, nil
}
