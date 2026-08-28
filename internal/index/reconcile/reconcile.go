package reconcile

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/fs"
	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
)

const (
	DefaultWorkers    = 128
	DefaultQueueDepth = 50_000
	DefaultBatchSize  = 5_000
)

type Options struct {
	Workers    int
	QueueDepth int
	BatchSize  int
}

func (options Options) withDefaults() (Options, error) {
	if options.Workers == 0 {
		options.Workers = DefaultWorkers
	}
	if options.QueueDepth == 0 {
		options.QueueDepth = DefaultQueueDepth
	}
	if options.BatchSize == 0 {
		options.BatchSize = DefaultBatchSize
	}
	if options.Workers < 1 || options.QueueDepth < 1 || options.BatchSize < 1 || options.BatchSize > 10_000 {
		return Options{}, fmt.Errorf("invalid reconciliation worker, queue, or batch limit")
	}
	return options, nil
}

type Metrics struct {
	Scanned    uint64 `json:"scanned"`
	Reused     uint64 `json:"reused"`
	Changed    uint64 `json:"changed"`
	Deferred   uint64 `json:"deferred"`
	Failed     uint64 `json:"failed"`
	Reconciled uint64 `json:"reconciled"`
}

type Store interface {
	Get(context.Context, []byte) ([]byte, bool, error)
	MultiGet(context.Context, [][]byte) ([]daemon.KeyValue, []bool, error)
	ScanPrefix(context.Context, []byte, []byte, uint32) ([]daemon.KeyValue, bool, error)
	AllocateRevision(context.Context) (uint64, error)
	PublishReconciledRevision(context.Context, daemon.ReconciledRevision) error
	RecordCrawlDebtFailure(context.Context, [][]byte, string) error
	ResolveCrawlDebt(context.Context, [][]byte) error
	Put(context.Context, []byte, []byte, bool) error
}

type statFS interface {
	Lstat(string) (*fs.ExtendedFileInfo, error)
	Dir(string) string
}

type workItem struct {
	snapshotPath string
	sourcePath   string
	node         data.Node
}

type identity struct {
	fsid  uint32
	inode uint64
}

type preparedItem struct {
	workItem
	stat       fs.ExtendedFileInfo
	parent     identity
	identity   identity
	debtKeys   [][]byte
	deferred   bool
	prepareErr error
	ParentCount     uint16
	HardlinkParents []schema.HardlinkParentRef
}

type publishedItem struct {
	identity     identity
	key          []byte
	typeID       schema.NodeType
	snapshotPath string
}

type Reconciler struct {
	ctx        context.Context
	filesystem statFS
	store      Store
	options    Options
	input      chan *workItem
	prepared   chan preparedItem
	pool       sync.Pool
	workers    sync.WaitGroup
	writerDone chan struct{}
	closeOnce  sync.Once
	mu         sync.Mutex
	errors     []error
	debtByPath map[string][][]byte

	scanned    atomic.Uint64
	reused     atomic.Uint64
	changed    atomic.Uint64
	deferred   atomic.Uint64
	failed     atomic.Uint64
	reconciled atomic.Uint64
}

func New(ctx context.Context, filesystem statFS, store Store, options Options) (*Reconciler, error) {
	if filesystem == nil || store == nil {
		return nil, fmt.Errorf("reconciliation requires a filesystem and store")
	}
	options, err := options.withDefaults()
	if err != nil {
		return nil, err
	}
	reconciler := &Reconciler{
		ctx: ctx, filesystem: filesystem, store: store, options: options,
		input: make(chan *workItem, options.QueueDepth), prepared: make(chan preparedItem, options.QueueDepth),
		writerDone: make(chan struct{}), debtByPath: make(map[string][][]byte),
	}
	reconciler.pool.New = func() any { return new(workItem) }
	if err := reconciler.loadPendingDebt(); err != nil {
		return nil, err
	}
	for range options.Workers {
		reconciler.workers.Add(1)
		go reconciler.statWorker()
	}
	go func() {
		reconciler.workers.Wait()
		close(reconciler.prepared)
	}()
	go reconciler.writer()
	return reconciler, nil
}

// Observe copies a completed backup node into the bounded scanner queue. It is
// safe for concurrent use and intentionally blocks to apply backpressure.
func (reconciler *Reconciler) Observe(snapshotPath, sourcePath string, node *data.Node) {
	if node == nil {
		return
	}
	item := reconciler.pool.Get().(*workItem)
	item.snapshotPath, item.sourcePath, item.node = snapshotPath, sourcePath, *node
	item.node.Content = append(item.node.Content[:0:0], node.Content...)
	if node.Subtree != nil {
		subtree := *node.Subtree
		item.node.Subtree = &subtree
	}
	select {
	case reconciler.input <- item:
	case <-reconciler.ctx.Done():
		reconciler.release(item)
		reconciler.recordError(reconciler.ctx.Err())
	}
}

// Close drains the scanner and writer and reports all item failures.
func (reconciler *Reconciler) Close() error {
	reconciler.closeOnce.Do(func() { close(reconciler.input) })
	<-reconciler.writerDone
	reconciler.mu.Lock()
	defer reconciler.mu.Unlock()
	return errors.Join(reconciler.errors...)
}

func (reconciler *Reconciler) Metrics() Metrics {
	return Metrics{
		Scanned: reconciler.scanned.Load(), Reused: reconciler.reused.Load(), Changed: reconciler.changed.Load(),
		Deferred: reconciler.deferred.Load(), Failed: reconciler.failed.Load(), Reconciled: reconciler.reconciled.Load(),
	}
}

// CanReuse permits the archiver fast path only for a complete verified inode
// whose live metadata and ordered content match the previous backup node.
func (reconciler *Reconciler) CanReuse(snapshotPath, sourcePath string, info *fs.ExtendedFileInfo, previous *data.Node) bool {
	if info == nil || previous == nil || previous.Type != data.NodeTypeFile {
		return false
	}
	identity, err := statIdentity(info)
	if err != nil {
		return false
	}
	record, content, found, err := reconciler.currentInode(identity)
	if err != nil {
		return false
	}
	if !found || record.Freshness != schema.FreshnessVerified {
		return false
	}
	required := schema.KnownMTime | schema.KnownCTime | schema.KnownSize | schema.KnownMode | schema.KnownUID | schema.KnownGID
	if record.Known&required != required || record.MTime != info.ModTime.UnixNano() || record.CTime != info.ChangeTime.UnixNano() ||
		record.Size != uint64(info.Size) || record.Mode != uint32(info.Mode) || record.UID != info.UID || record.GID != info.GID {
		return false
	}
	if info.Links <= 1 {
		parentInfo, statErr := reconciler.filesystem.Lstat(reconciler.filesystem.Dir(sourcePath))
		if statErr != nil {
			return false
		}
		parent, identityErr := statIdentity(parentInfo)
		if identityErr != nil || record.Known&schema.KnownParent == 0 || record.ParentInode != parent.inode || parent.fsid != identity.fsid {
			return false
		}
	}
	if normalizeSnapshotPath(record.SourcePath) != normalizeSnapshotPath(snapshotPath) && info.Links <= 1 {
		return false
	}
	if len(content) != len(previous.Content) {
		return false
	}
	for index, id := range content {
		if id != schema.ID(previous.Content[index]) {
			return false
		}
	}
	return true
}

func (reconciler *Reconciler) currentInode(identity identity) (schema.InodeRevision, []schema.ID, bool, error) {
	values, currentFound, err := reconciler.store.MultiGet(reconciler.ctx, [][]byte{schema.CurrentInodeKey(identity.fsid, identity.inode)})
	if err != nil || !currentFound[0] {
		return schema.InodeRevision{}, nil, false, err
	}
	pointer, err := schema.UnmarshalCurrentPointer(values[0].Value)
	if err != nil {
		return schema.InodeRevision{}, nil, false, err
	}
	value, revisionFound, err := reconciler.store.Get(reconciler.ctx, pointer.RecordKey)
	if err != nil || !revisionFound {
		return schema.InodeRevision{}, nil, false, err
	}
	record, err := schema.UnmarshalInodeRevision(value)
	if err != nil {
		return schema.InodeRevision{}, nil, false, err
	}
	content, err := reconciler.contentIDs(record)
	return record, content, true, err
}

func (reconciler *Reconciler) contentIDs(record schema.InodeRevision) ([]schema.ID, error) {
	switch record.ContentMode {
	case schema.ContentNone:
		return nil, nil
	case schema.ContentInline:
		return append([]schema.ID(nil), record.ContentIDs...), nil
	case schema.ContentManifestRef:
		segmentCount := (uint64(record.ContentCount) + schema.DefaultContentSegmentIDs - 1) / schema.DefaultContentSegmentIDs
		keys := make([][]byte, segmentCount)
		for index := range keys {
			keys[index] = schema.ContentManifestKey(record.ContentManifestID, uint32(index))
		}
		values, found, err := reconciler.store.MultiGet(reconciler.ctx, keys)
		if err != nil {
			return nil, err
		}
		segments := make([]schema.ContentManifest, segmentCount)
		for index := range segments {
			if !found[index] {
				return nil, fmt.Errorf("content manifest segment %d is missing", index)
			}
			segments[index], err = schema.UnmarshalContentManifest(values[index].Value)
			if err != nil {
				return nil, err
			}
		}
		return schema.AssembleContent(record.ContentManifestID, segments)
	default:
		return nil, fmt.Errorf("invalid content mode")
	}
}

func (reconciler *Reconciler) loadPendingDebt() error {
	var after []byte
	for {
		entries, done, err := reconciler.store.ScanPrefix(reconciler.ctx, []byte("q:"), after, uint32(min(reconciler.options.BatchSize, 10_000)))
		if err != nil {
			return err
		}
		for _, entry := range entries {
			debt, decodeErr := schema.UnmarshalCrawlDebtRecord(entry.Value)
			if decodeErr != nil {
				return decodeErr
			}
			if debt.Status == schema.DebtPending && len(debt.PathOrTree) > 0 {
				path := normalizeSnapshotPath(string(debt.PathOrTree))
				reconciler.debtByPath[path] = append(reconciler.debtByPath[path], append([]byte(nil), entry.Key...))
			}
		}
		if done {
			return nil
		}
		if len(entries) == 0 {
			return fmt.Errorf("crawl-debt scan made no progress")
		}
		after = append(after[:0], entries[len(entries)-1].Key...)
	}
}

func (reconciler *Reconciler) statWorker() {
	defer reconciler.workers.Done()
	for item := range reconciler.input {
		prepared := preparedItem{workItem: *item, debtKeys: reconciler.debtByPath[normalizeSnapshotPath(item.snapshotPath)]}
		info, err := reconciler.filesystem.Lstat(item.sourcePath)
		if err == nil {
			prepared.stat = *info
			prepared.identity, err = statIdentity(info)
			prepared.deferred = errors.Is(err, errUnavailableIdentity)
		}
		if err == nil {
			parentInfo, parentErr := reconciler.filesystem.Lstat(reconciler.filesystem.Dir(item.sourcePath))
			if parentErr != nil {
				err = parentErr
			} else {
				prepared.parent, err = statIdentity(parentInfo)
				prepared.deferred = errors.Is(err, errUnavailableIdentity)
			}
		}
		if err == nil && prepared.identity.fsid != prepared.parent.fsid {
			err = fmt.Errorf("cross-filesystem parent relationship")
			prepared.deferred = true
		}
		if err == nil && prepared.identity == prepared.parent && info.Mode.IsDir() {
			err = fmt.Errorf("directory %q forms a parent cycle", item.sourcePath)
		}
		prepared.prepareErr = err
		reconciler.scanned.Add(1)
		select {
		case reconciler.prepared <- prepared:
		case <-reconciler.ctx.Done():
			reconciler.recordError(reconciler.ctx.Err())
		}
		reconciler.release(item)
	}
}

func (reconciler *Reconciler) writer() {
	defer close(reconciler.writerDone)
	var directories []preparedItem
	hardlinks := make(map[identity][]preparedItem)
	published := make(map[string]publishedItem)
	batch := make([]preparedItem, 0, reconciler.options.BatchSize)
	for item := range reconciler.prepared {
		batch = append(batch, item)
		if len(batch) == reconciler.options.BatchSize {
			reconciler.processBatch(batch, &directories, hardlinks, published)
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		reconciler.processBatch(batch, &directories, hardlinks, published)
	}
	reconciler.publishHardlinks(hardlinks, published)
	reconciler.publishDirectories(directories, published)
}

func (reconciler *Reconciler) processBatch(batch []preparedItem, directories *[]preparedItem, hardlinks map[identity][]preparedItem, published map[string]publishedItem) {
	for _, item := range batch {
		if item.deferred {
			reconciler.deferred.Add(1)
			if err := reconciler.writeDeferredDebt(item.snapshotPath, item.prepareErr); err != nil {
				reconciler.fail(item.sourcePath, err, item.debtKeys)
			}
			continue
		}
		if item.prepareErr != nil {
			reconciler.fail(item.sourcePath, item.prepareErr, item.debtKeys)
			continue
		}
		if item.stat.Mode.IsDir() {
			*directories = append(*directories, item)
			continue
		}
		if item.stat.Links > 1 {
			hardlinks[item.identity] = append(hardlinks[item.identity], item)
			continue
		}
		reconciler.publishInode(item, published)
	}
}

func (reconciler *Reconciler) publishHardlinks(hardlinks map[identity][]preparedItem, published map[string]publishedItem) {
	hardlinkIDs := make([]identity, 0, len(hardlinks))
	for id := range hardlinks {
		hardlinkIDs = append(hardlinkIDs, id)
	}
	sort.Slice(hardlinkIDs, func(left, right int) bool {
		if hardlinkIDs[left].fsid != hardlinkIDs[right].fsid {
			return hardlinkIDs[left].fsid < hardlinkIDs[right].fsid
		}
		return hardlinkIDs[left].inode < hardlinkIDs[right].inode
	})
	for _, hardlinkID := range hardlinkIDs {
		links := hardlinks[hardlinkID]
		sort.Slice(links, func(left, right int) bool { return links[left].sourcePath < links[right].sourcePath })
		if err := validateHardlinks(links); err != nil {
			for _, item := range links {
				reconciler.fail(item.sourcePath, err, item.debtKeys)
			}
			continue
		}
		links[0].debtKeys = mergeDebtKeys(links)
		// Compute multi-parent inode metadata
		links[0].ParentCount = uint16(len(links))
		links[0].HardlinkParents = make([]schema.HardlinkParentRef, 0, len(links))
		for _, link := range links {
			links[0].HardlinkParents = append(links[0].HardlinkParents, schema.HardlinkParentRef{
				ParentInode: link.parent.inode,
				Name:        normalizeSnapshotPath(link.snapshotPath),
			})
		}
		key, err := reconciler.publishInodeRecord(links[0], true)
		if err != nil {
			for _, item := range links {
				reconciler.fail(item.sourcePath, err, item.debtKeys)
			}
			continue
		}
		for _, item := range links {
			published[item.sourcePath] = publishedItem{identity: item.identity, key: key, typeID: nodeType(item.node.Type), snapshotPath: item.snapshotPath}
		}
	}
}

func (reconciler *Reconciler) publishDirectories(directories []preparedItem, published map[string]publishedItem) {
	if err := validateLiveDirectoryParents(directories); err != nil {
		reconciler.fail("directories", err)
		return
	}
	sort.Slice(directories, func(left, right int) bool {
		leftDepth, rightDepth := pathDepth(directories[left].sourcePath), pathDepth(directories[right].sourcePath)
		if leftDepth != rightDepth {
			return leftDepth > rightDepth
		}
		return directories[left].sourcePath < directories[right].sourcePath
	})
	for _, directory := range directories {
		children := make([]schema.DirectoryChild, 0)
		for sourcePath, child := range published {
			if reconciler.filesystem.Dir(sourcePath) != directory.sourcePath {
				continue
			}
			if child.identity.fsid != directory.identity.fsid {
				reconciler.deferred.Add(1)
				if err := reconciler.writeDeferredDebt(child.snapshotPath, fmt.Errorf("cross-filesystem directory child")); err != nil {
					reconciler.fail(child.snapshotPath, err)
				}
				continue
			}
			children = append(children, schema.DirectoryChild{Name: filepath.Base(child.snapshotPath), Inode: child.identity.inode, Type: child.typeID, MetadataKey: child.key})
		}
		record := schema.DirectoryRevision{
			ParentInode: directory.parent.inode, Children: children,
			MTime: directory.stat.ModTime.UnixNano(), CTime: directory.stat.ChangeTime.UnixNano(), Size: uint64(directory.stat.Size),
			Mode: uint32(directory.stat.Mode), UID: directory.stat.UID, GID: directory.stat.GID,
			Known:      schema.KnownMTime | schema.KnownCTime | schema.KnownSize | schema.KnownMode | schema.KnownUID | schema.KnownGID | schema.KnownParent | schema.KnownPath,
			SourcePath: normalizeSnapshotPath(directory.snapshotPath), Freshness: schema.FreshnessVerified,
		}
		value, err := record.MarshalBinary()
		if err != nil {
			reconciler.fail(directory.sourcePath, err, directory.debtKeys)
			continue
		}
		key, reused, err := reconciler.publishRecord(directory, value, true, nil)
		if err != nil {
			reconciler.fail(directory.sourcePath, err, directory.debtKeys)
			continue
		}
		if reused {
			reconciler.reused.Add(1)
		} else {
			reconciler.changed.Add(1)
			reconciler.reconciled.Add(1)
		}
		published[directory.sourcePath] = publishedItem{identity: directory.identity, key: key, typeID: schema.NodeDirectory, snapshotPath: directory.snapshotPath}
	}
}

func (reconciler *Reconciler) publishInode(item preparedItem, published map[string]publishedItem) {
	key, err := reconciler.publishInodeRecord(item, false)
	if err != nil {
		reconciler.fail(item.sourcePath, err, item.debtKeys)
		return
	}
	published[item.sourcePath] = publishedItem{identity: item.identity, key: key, typeID: nodeType(item.node.Type), snapshotPath: item.snapshotPath}
}

func validateHardlinks(links []preparedItem) error {
	if len(links) == 0 {
		return fmt.Errorf("empty hardlink group")
	}
	first := links[0]
	for _, item := range links[1:] {
		if item.stat.Size != first.stat.Size || item.stat.Mode != first.stat.Mode || item.stat.UID != first.stat.UID || item.stat.GID != first.stat.GID ||
			!item.stat.ModTime.Equal(first.stat.ModTime) || !item.stat.ChangeTime.Equal(first.stat.ChangeTime) || len(item.node.Content) != len(first.node.Content) {
			return fmt.Errorf("hardlink aliases have inconsistent metadata or content")
		}
		for index := range item.node.Content {
			if item.node.Content[index] != first.node.Content[index] {
				return fmt.Errorf("hardlink aliases have inconsistent metadata or content")
			}
		}
	}
	return nil
}

func mergeDebtKeys(links []preparedItem) [][]byte {
	seen := make(map[string]struct{})
	var result [][]byte
	for _, item := range links {
		for _, key := range item.debtKeys {
			if _, found := seen[string(key)]; found {
				continue
			}
			seen[string(key)] = struct{}{}
			result = append(result, key)
		}
	}
	sort.Slice(result, func(left, right int) bool { return bytes.Compare(result[left], result[right]) < 0 })
	return result
}

func (reconciler *Reconciler) publishInodeRecord(item preparedItem, hardlink bool) ([]byte, error) {
	record := schema.InodeRevision{
		ParentInode: item.parent.inode, MTime: item.stat.ModTime.UnixNano(), CTime: item.stat.ChangeTime.UnixNano(),
		Size: uint64(item.stat.Size), Mode: uint32(item.stat.Mode), UID: item.stat.UID, GID: item.stat.GID,
		Known:      schema.KnownMTime | schema.KnownCTime | schema.KnownSize | schema.KnownMode | schema.KnownUID | schema.KnownGID | schema.KnownParent | schema.KnownPath,
		SourcePath: normalizeSnapshotPath(item.snapshotPath), Freshness: schema.FreshnessVerified,
	}
	// Set ParentCount (defaults to 1 for single-parent files)
	record.ParentCount = item.ParentCount
	if item.ParentCount == 0 {
		record.ParentCount = 1
	}
	if hardlink {
		record.ParentInode = 0
		record.SourcePath = ""
		record.Known &^= schema.KnownParent | schema.KnownPath
	}
	content := make([]schema.ID, len(item.node.Content))
	for index, id := range item.node.Content {
		content[index] = schema.ID(id)
	}
	if len(content) > 0 {
		record.ContentCount = uint32(len(content))
		if len(content) <= schema.MaxInlineContentIDs {
			record.ContentMode, record.ContentIDs = schema.ContentInline, content
		} else {
			record.ContentMode, record.ContentManifestID = schema.ContentManifestRef, schema.ContentManifestID(content)
		}
	}
	value, err := record.MarshalBinary()
	if err != nil {
		return nil, err
	}
	key, reused, err := reconciler.publishRecord(item, value, false, content)
	if err != nil {
		return nil, err
	}
	if reused {
		reconciler.reused.Add(1)
	} else {
		reconciler.changed.Add(1)
		reconciler.reconciled.Add(1)
	}
	return key, nil
}

func (reconciler *Reconciler) publishRecord(item preparedItem, value []byte, directory bool, content []schema.ID) ([]byte, bool, error) {
	currentKey := schema.CurrentInodeKey(item.identity.fsid, item.identity.inode)
	if directory {
		currentKey = schema.CurrentDirectoryKey(item.identity.fsid, item.identity.inode)
	}
	values, found, err := reconciler.store.MultiGet(reconciler.ctx, [][]byte{currentKey})
	if err != nil {
		return nil, false, err
	}
	if found[0] {
		pointer, decodeErr := schema.UnmarshalCurrentPointer(values[0].Value)
		if decodeErr != nil {
			return nil, false, decodeErr
		}
		existing, valueFound, getErr := reconciler.store.MultiGet(reconciler.ctx, [][]byte{pointer.RecordKey})
		if getErr != nil {
			return nil, false, getErr
		}
		if valueFound[0] && bytes.Equal(existing[0].Value, value) {
			if directory {
				if err := reconciler.store.ResolveCrawlDebt(reconciler.ctx, item.debtKeys); err != nil {
					return nil, false, err
				}
				return pointer.RecordKey, true, nil
			}
			record, decodeErr := schema.UnmarshalInodeRevision(existing[0].Value)
			if decodeErr == nil && record.Freshness == schema.FreshnessVerified {
				existingContent, contentErr := reconciler.contentIDs(record)
				if contentErr == nil && equalSchemaIDs(existingContent, content) {
					if err := reconciler.store.ResolveCrawlDebt(reconciler.ctx, item.debtKeys); err != nil {
						return nil, false, err
					}
					return pointer.RecordKey, true, nil
				}
			}
		}
	}
	revision, err := reconciler.store.AllocateRevision(reconciler.ctx)
	if err != nil {
		return nil, false, err
	}
	revisionKey := schema.InodeRevisionKey(item.identity.fsid, item.identity.inode, revision)
	if directory {
		revisionKey = schema.DirectoryRevisionKey(item.identity.fsid, item.identity.inode, revision)
	}
	if err := reconciler.store.PublishReconciledRevision(reconciler.ctx, daemon.ReconciledRevision{
		CurrentKey: currentKey, RevisionKey: revisionKey, RevisionValue: value, Revision: revision,
		ContentIDs: content, DebtKeys: item.debtKeys,
		ParentCount: item.ParentCount, HardlinkParents: item.HardlinkParents,
	}); err != nil {
		return nil, false, err
	}
	return revisionKey, false, nil
}

func equalSchemaIDs(left, right []schema.ID) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func validateLiveDirectoryParents(items []preparedItem) error {
	parents := make(map[identity]identity, len(items))
	for _, item := range items {
		parents[item.identity] = item.parent
	}
	for start := range parents {
		seen := map[identity]struct{}{}
		for current := start; current != (identity{}); current = parents[current] {
			if _, found := seen[current]; found {
				return fmt.Errorf("directory parent cycle at filesystem %d inode %d", current.fsid, current.inode)
			}
			seen[current] = struct{}{}
			if _, found := parents[current]; !found {
				break
			}
		}
	}
	return nil
}

var errUnavailableIdentity = errors.New("live filesystem identity is unavailable")

func statIdentity(info *fs.ExtendedFileInfo) (identity, error) {
	if info == nil || info.DeviceID == 0 || info.DeviceID > math.MaxUint32 || info.Inode == 0 {
		return identity{}, errUnavailableIdentity
	}
	return identity{fsid: uint32(info.DeviceID), inode: info.Inode}, nil
}

func nodeType(value data.NodeType) schema.NodeType {
	switch value {
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

func normalizeSnapshotPath(value string) string {
	return strings.TrimPrefix(filepath.ToSlash(filepath.Clean(value)), "/")
}

func pathDepth(value string) int {
	return strings.Count(filepath.Clean(value), string(filepath.Separator))
}

func (reconciler *Reconciler) release(item *workItem) {
	*item = workItem{}
	reconciler.pool.Put(item)
}

func (reconciler *Reconciler) fail(path string, err error, matchedDebt ...[][]byte) {
	reconciler.failed.Add(1)
	debtKeys := reconciler.debtByPath[normalizeSnapshotPath(path)]
	if len(matchedDebt) > 0 {
		debtKeys = matchedDebt[0]
	}
	if len(debtKeys) > 0 {
		if debtErr := reconciler.store.RecordCrawlDebtFailure(reconciler.ctx, debtKeys, errorClass(err)); debtErr != nil {
			reconciler.recordError(fmt.Errorf("update crawl debt for %q: %w", path, debtErr))
		}
	}
	reconciler.recordError(fmt.Errorf("reconcile %q: %w", path, err))
}

func errorClass(err error) string {
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	if errors.Is(err, os.ErrNotExist) {
		return "not-found"
	}
	return "reconciliation-failed"
}

func (reconciler *Reconciler) writeDeferredDebt(snapshotPath string, cause error) error {
	normalized := normalizeSnapshotPath(snapshotPath)
	work := schema.ID(sha256.Sum256([]byte("phase5-deferred:" + normalized + ":" + errorClass(cause))))
	key := schema.CrawlDebtKey(schema.ID{}, work)
	record := schema.CrawlDebtRecord{
		PathOrTree: []byte(normalized), Reason: schema.DebtMissingInode, Status: schema.DebtPending,
		LastAttemptUnixNano: time.Now().UnixNano(), ErrorClass: errorClass(cause),
	}
	if existing, found, err := reconciler.store.Get(reconciler.ctx, key); err != nil {
		return err
	} else if found {
		previous, decodeErr := schema.UnmarshalCrawlDebtRecord(existing)
		if decodeErr != nil {
			return decodeErr
		}
		record.RetryCount = previous.RetryCount
		if record.RetryCount < math.MaxUint32 {
			record.RetryCount++
		}
	}
	encoded, err := record.MarshalBinary()
	if err != nil {
		return err
	}
	return reconciler.store.Put(reconciler.ctx, key, encoded, true)
}

func (reconciler *Reconciler) recordError(err error) {
	if err == nil {
		return
	}
	reconciler.mu.Lock()
	reconciler.errors = append(reconciler.errors, err)
	reconciler.mu.Unlock()
}
