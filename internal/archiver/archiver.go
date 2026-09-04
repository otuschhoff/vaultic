package archiver

import (
	"context"
	"fmt"
	"os"
	"path"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/otuschhoff/vaultic/internal/crawl"
	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/debug"
	"github.com/otuschhoff/vaultic/internal/errors"
	"github.com/otuschhoff/vaultic/internal/feature"
	"github.com/otuschhoff/vaultic/internal/fs"
	"github.com/otuschhoff/vaultic/internal/vaultic"
	"golang.org/x/sync/errgroup"
)

// SelectByNameFunc returns true for all items that should be included (files and
// dirs). If false is returned, files are ignored and dirs are not even walked.
type SelectByNameFunc func(item string) bool

// SelectFunc returns true for all items that should be included (files and
// dirs). If false is returned, files are ignored and dirs are not even walked.
type SelectFunc func(item string, fi *fs.ExtendedFileInfo, fs fs.FS) bool

// ErrorFunc is called when an error during archiving occurs. When nil is
// returned, the archiver continues, otherwise it aborts and passes the error
// up the call stack.
type ErrorFunc func(file string, err error) error

// ItemStats collects some statistics about a particular file or directory.
type ItemStats struct {
	DataBlobs      int    // number of new data blobs added for this item
	DataSize       uint64 // sum of the sizes of all new data blobs
	DataSizeInRepo uint64 // sum of the bytes added to the repo (including compression and crypto overhead)
	TreeBlobs      int    // number of new tree blobs added for this item
	TreeSize       uint64 // sum of the sizes of all new tree blobs
	TreeSizeInRepo uint64 // sum of the bytes added to the repo (including compression and crypto overhead)
}

type ChangeStats struct {
	New       uint
	Changed   uint
	Unchanged uint
}

type Summary struct {
	BackupStart    time.Time
	BackupEnd      time.Time
	Files, Dirs    ChangeStats
	ProcessedBytes uint64
	ItemStats
}

// Add adds other to the current ItemStats.
func (s *ItemStats) Add(other ItemStats) {
	s.DataBlobs += other.DataBlobs
	s.DataSize += other.DataSize
	s.DataSizeInRepo += other.DataSizeInRepo
	s.TreeBlobs += other.TreeBlobs
	s.TreeSize += other.TreeSize
	s.TreeSizeInRepo += other.TreeSizeInRepo
}

// toNoder returns a data.Node for a File.
type toNoder interface {
	ToNode(ignoreXattrListError bool, warnf func(format string, args ...any)) (*data.Node, error)
}

type archiverRepo interface {
	vaultic.Loader
	vaultic.WithBlobUploader
	vaultic.SaverUnpacked[vaultic.WriteableFileType]

	ChunkerFactory() vaultic.ChunkerFactory
}

// Archiver saves a directory structure to the repo.
//
// An Archiver has a number of worker goroutines handling saving the different
// data structures to the repository, the details are implemented by the
// fileSaver, blobSaver, and treeSaver types.
//
// The main goroutine (the one calling Snapshot()) traverses the directory tree
// and delegates all work to these worker pools. They return a futureNode which
// can be resolved later, by calling Wait() on it.
type Archiver struct {
	Repo         archiverRepo
	SelectByName SelectByNameFunc
	Select       SelectFunc
	// MandatorySelect applies policy filters even to explicit backup roots.
	MandatorySelect SelectFunc
	FS              fs.FS
	Options         Options

	fileSaver     *fileSaver
	treeSaver     *treeSaver
	cwalkManifest *crawl.DirectoryManifest
	mu            sync.Mutex
	summary       *Summary

	// Error is called for all errors that occur during backup.
	Error ErrorFunc

	// CompleteItem is called for all files and dirs once they have been
	// processed successfully. The parameter item contains the path as it will
	// be in the snapshot after saving. s contains some statistics about this
	// particular file/dir.
	//
	// Once reading a file has completed successfully (but not saving it yet),
	// CompleteItem will be called with a zero ItemAction.
	//
	// CompleteItem may be called asynchronously from several different
	// goroutines!
	CompleteItem func(item string, action ItemAction, s ItemStats, d time.Duration)

	// ReconcileNode is called after a file or directory has been processed and
	// its final metadata and content references are available. Implementations
	// must copy retained data and return promptly; calls may be concurrent.
	ReconcileNode func(snapshotPath, sourcePath string, node *data.Node)
	// BeforeSnapshot runs after all packs and final nodes are durable but before
	// the snapshot object is saved.
	BeforeSnapshot func() error

	// ReuseNode can reject the legacy unchanged-file fast path when an
	// additional metadata authority requires the file to be read again.
	ReuseNode func(snapshotPath, sourcePath string, info *fs.ExtendedFileInfo, previous *data.Node) bool
	// ReuseSubtree permits a verified change source to reuse an unchanged
	// parent directory without listing its descendants.
	ReuseSubtree func(snapshotPath, sourcePath string, previous *data.Node) bool

	// StartFile is called when a file is being processed by a worker.
	StartFile func(filename string)

	// CompleteBlob is called for all saved blobs for files.
	CompleteBlob func(bytes uint64)

	// WithAtime configures if the access time for files and directories should
	// be saved. Enabling it may result in much metadata, so it's off by
	// default.
	WithAtime bool

	// Flags controlling change detection. See doc/040_backup.rst for details.
	ChangeIgnoreFlags uint

	// for excluded items
	ExcludedItem func(path string)
}

// Flags for the ChangeIgnoreFlags bitfield.
const (
	ChangeIgnoreCtime = 1 << iota
	ChangeIgnoreInode
)

// Options is used to configure the archiver.
type Options struct {
	// ReadConcurrency sets how many files are read in concurrently. If
	// it's set to zero, at most two files are read in concurrently (which
	// turned out to be a good default for most situations).
	ReadConcurrency uint

	// SaveTreeConcurrency sets how many trees are marshalled and saved to the
	// repo concurrently.
	SaveTreeConcurrency uint

	// CWalkConcurrency enables a disk-backed cwalk directory manifest using
	// this many workers. Zero retains direct sequential directory reads.
	CWalkConcurrency int
	// CWalkQueue bounds directory records waiting for manifest persistence.
	CWalkQueue int
	// CWalkRoots optionally limits manifest discovery to selective changed roots.
	// Empty means the snapshot targets.
	CWalkRoots []string
}

// applyDefaults returns a copy of o with the default options set for all unset
// fields.
func (o Options) applyDefaults() Options {
	if o.ReadConcurrency == 0 {
		// two is a sweet spot for almost all situations. We've done some
		// experiments documented here:
		// https://github.com/borgbackup/borg/issues/3500
		o.ReadConcurrency = 2
	}

	if o.SaveTreeConcurrency == 0 {
		// can either wait for a file, wait for a tree, serialize a tree or wait for saveblob
		// the last two are cpu-bound and thus mutually exclusive.
		// Also allow waiting for ReadConcurrency files, this is the maximum of files
		// which currently can be in progress. The main backup loop blocks when trying to queue
		// more files to read.
		o.SaveTreeConcurrency = uint(runtime.GOMAXPROCS(0)) + o.ReadConcurrency
	}

	return o
}

// New initializes a new archiver.
func New(repo archiverRepo, filesystem fs.FS, opts Options) *Archiver {
	arch := &Archiver{
		Repo:            repo,
		SelectByName:    func(_ string) bool { return true },
		Select:          func(_ string, _ *fs.ExtendedFileInfo, _ fs.FS) bool { return true },
		MandatorySelect: func(_ string, _ *fs.ExtendedFileInfo, _ fs.FS) bool { return true },
		FS:              filesystem,
		Options:         opts.applyDefaults(),

		CompleteItem:   func(string, ItemAction, ItemStats, time.Duration) {},
		ReconcileNode:  func(string, string, *data.Node) {},
		BeforeSnapshot: func() error { return nil },
		ReuseNode:      func(string, string, *fs.ExtendedFileInfo, *data.Node) bool { return true },
		ReuseSubtree:   func(string, string, *data.Node) bool { return false },
		StartFile:      func(string) {},
		CompleteBlob:   func(uint64) {},
		ExcludedItem:   func(string) {},
	}

	return arch
}

// error calls arch.Error if it is set and the error is different from context.Canceled.
func (arch *Archiver) error(item string, err error) error {
	if arch.Error == nil || err == nil {
		return err
	}

	if err == context.Canceled {
		return err
	}

	// not all errors include the filepath, thus add it if it is missing
	if !strings.Contains(err.Error(), item) {
		err = fmt.Errorf("%v: %w", item, err)
	}

	errf := arch.Error(item, err)
	if err != errf {
		debug.Log("item %v: error was filtered by handler, before: %q, after: %v", item, err, errf)
	}
	return errf
}

func (arch *Archiver) trackItem(item string, previous, current *data.Node, s ItemStats, d time.Duration) {
	action := itemProgressAction(previous, current)
	arch.CompleteItem(item, action, s, d)

	arch.mu.Lock()
	defer arch.mu.Unlock()

	arch.summary.ItemStats.Add(s)

	if action.IsZero() {
		// last item or an error occurred
		return
	}

	arch.summary.ProcessedBytes += current.Size

	switch action {
	case ActionDirNew:
		arch.summary.Dirs.New++
	case ActionDirUnchanged:
		arch.summary.Dirs.Unchanged++
	case ActionDirModified:
		arch.summary.Dirs.Changed++

	case ActionFileNew:
		arch.summary.Files.New++
	case ActionFileUnchanged:
		arch.summary.Files.Unchanged++
	case ActionFileModified:
		arch.summary.Files.Changed++
	}
}

func (arch *Archiver) reconcileNode(snapshotPath, sourcePath string, node *data.Node) {
	if node != nil {
		arch.ReconcileNode(snapshotPath, sourcePath, node)
	}
}

// nodeFromFileInfo returns the vaultic node from an os.FileInfo.
func (arch *Archiver) nodeFromFileInfo(snPath, filename string, meta toNoder, ignoreXattrListError bool) (*data.Node, error) {
	node, err := meta.ToNode(ignoreXattrListError, func(format string, args ...any) {
		_ = arch.error(filename, fmt.Errorf(format, args...))
	})
	// node does not exist. This prevents all further processing for this file.
	// If an error and a node are returned, then preserve as much data as possible (see below).
	if err != nil && node == nil {
		return nil, err
	}
	if !arch.WithAtime {
		node.AccessTime = node.ModTime
	}
	if feature.Flag.Enabled(feature.DeviceIDForHardlinks) {
		if node.Links == 1 || node.Type == data.NodeTypeDir {
			// the DeviceID is only necessary for hardlinked files
			// when using subvolumes or snapshots their deviceIDs tend to change which causes
			// vaultic to upload new tree blobs
			node.DeviceID = 0
		}
	}
	// overwrite name to match that within the snapshot
	node.Name = path.Base(snPath)
	// do not filter error for nodes of irregular or invalid type
	if node.Type != data.NodeTypeIrregular && node.Type != data.NodeTypeInvalid && err != nil {
		err = fmt.Errorf("incomplete metadata for %v: %w", filename, err)
		return node, arch.error(filename, err)
	}
	return node, err
}

// loadSubtree tries to load the subtree referenced by node. In case of an error, nil is returned.
// If there is no node to load, then nil is returned without an error.
func (arch *Archiver) loadSubtree(ctx context.Context, node *data.Node) (data.TreeNodeIterator, error) {
	if node == nil || node.Type != data.NodeTypeDir || node.Subtree == nil {
		return nil, nil
	}

	tree, err := data.LoadTree(ctx, arch.Repo, *node.Subtree)
	if err != nil {
		debug.Log("unable to load tree %v: %v", node.Subtree.Str(), err)
		// a tree in the repository is not readable -> warn the user
		return nil, arch.wrapLoadTreeError(*node.Subtree, err)
	}

	return tree, nil
}

func (arch *Archiver) wrapLoadTreeError(id vaultic.ID, err error) error {
	if _, ok := arch.Repo.LookupBlobSize(vaultic.BlobHandle{Type: vaultic.TreeBlob, ID: id}); ok {
		err = errors.Errorf("tree %v could not be loaded; the repository could be damaged: %w", id, err)
	} else {
		err = errors.Errorf("tree %v is not known; the repository could be damaged, run `repair index` to try to repair it", id)
	}
	return err
}

// saveDir stores a directory in the repo and returns the node. snPath is the
// path within the current snapshot.
func (arch *Archiver) saveDir(
	ctx context.Context,
	snPath string,
	dir string,
	meta fs.File,
	previous data.TreeNodeIterator,
	complete fileCompleteFunc,
) (d futureNode, err error) {
	debug.Log("%v %v", snPath, dir)

	treeNode, names, err := arch.dirToNodeAndEntries(snPath, dir, meta)
	if err != nil {
		return futureNode{}, err
	}

	nodes := make([]futureNode, 0, len(names))

	finder := data.NewTreeFinder(previous)
	defer finder.Close()

	var lastExcluded string

	for _, name := range names {
		// test if context has been cancelled
		if ctx.Err() != nil {
			debug.Log("context has been cancelled, aborting")
			return futureNode{}, ctx.Err()
		}

		if name == lastExcluded {
			// Skip duplicate directory entry if it was already excluded.
			// This avoids printing errors about duplicate directory entries even though the entry in question is ignored.
			continue
		}

		pathname := arch.FS.Join(dir, name)
		oldNode, err := finder.Find(name)
		err = arch.error(pathname, err)
		if err != nil {
			return futureNode{}, err
		}
		snItem := join(snPath, name)
		if oldNode != nil && oldNode.Type == data.NodeTypeDir && oldNode.Subtree != nil && arch.ReuseSubtree(snItem, pathname, oldNode) {
			node := *oldNode
			nodes = append(nodes, newFutureNodeWithResult(futureNodeResult{snPath: snItem, target: pathname, node: &node}))
			continue
		}
		fn, excluded, err := arch.save(ctx, snItem, pathname, oldNode, false)

		// return error early if possible
		if err != nil {
			err = arch.error(pathname, err)
			if err == nil {
				// ignore error
				continue
			}

			return futureNode{}, err
		}

		if excluded {
			lastExcluded = name
			continue
		}

		nodes = append(nodes, fn)
	}

	fn := arch.treeSaver.Save(ctx, snPath, dir, treeNode, nodes, complete)

	return fn, nil
}

func (arch *Archiver) dirToNodeAndEntries(snPath, dir string, meta fs.File) (node *data.Node, names []string, err error) {
	node, err = arch.nodeFromFileInfo(snPath, dir, meta, false)
	if err != nil {
		return nil, nil, err
	}
	if node.Type != data.NodeTypeDir {
		return nil, nil, fmt.Errorf("directory %q changed type, refusing to archive", snPath)
	}

	if arch.cwalkManifest != nil {
		var found bool
		names, found, err = arch.cwalkManifest.Names(dir)
		if err != nil {
			return nil, nil, fmt.Errorf("read cwalk manifest for %v: %w", dir, err)
		}
		if found {
			sort.Strings(names)
			return node, names, nil
		}
	}
	err = meta.MakeReadable()
	if err != nil {
		return nil, nil, fmt.Errorf("openfile for readdirnames failed: %w", err)
	}
	names, err = meta.Readdirnames(-1)
	if err != nil {
		return nil, nil, fmt.Errorf("readdirnames %v failed: %w", dir, err)
	}
	sort.Strings(names)

	return node, names, nil
}

// futureNode holds a reference to a channel that returns a FutureNodeResult
// or a reference to an already existing result. If the result is available
// immediately, then storing a reference directly requires less memory than
// using the indirection via a channel.
type futureNode struct {
	ch  <-chan futureNodeResult
	res *futureNodeResult
}

type futureNodeResult struct {
	snPath, target string

	node  *data.Node
	stats ItemStats
	err   error
}

func newFutureNode() (futureNode, chan<- futureNodeResult) {
	ch := make(chan futureNodeResult, 1)
	return futureNode{ch: ch}, ch
}

func newFutureNodeWithResult(res futureNodeResult) futureNode {
	return futureNode{
		res: &res,
	}
}

func (fn *futureNode) take(ctx context.Context) futureNodeResult {
	if fn.res != nil {
		res := fn.res
		// free result
		fn.res = nil
		return *res
	}
	select {
	case res, ok := <-fn.ch:
		if ok {
			// free channel
			fn.ch = nil
			return res
		}
	case <-ctx.Done():
		return futureNodeResult{err: ctx.Err()}
	}
	return futureNodeResult{err: errors.Errorf("no result")}
}

// allBlobsPresent checks if all blobs (contents) of the given node are
// present in the index.
func (arch *Archiver) allBlobsPresent(previous *data.Node) bool {
	// check if all blobs are contained in index
	for _, id := range previous.Content {
		if _, ok := arch.Repo.LookupBlobSize(vaultic.BlobHandle{Type: vaultic.DataBlob, ID: id}); !ok {
			return false
		}
	}
	return true
}

// save saves a target (file or directory) to the repo. If the item is
// excluded, this function returns a nil node and error, with excluded set to
// true.
//
// Errors and completion needs to be handled by the caller.
//
// snPath is the path within the current snapshot.
//
// explicit is true when this path was a backup target (tree leaf with Explicit
// set); Excludes (`SelectByName` and `Select`) are skipped for that path only.
func (arch *Archiver) save(ctx context.Context, snPath, target string, previous *data.Node, explicit bool) (fn futureNode, excluded bool, err error) {
	start := time.Now()

	debug.Log("%v target %q, previous %v", snPath, target, previous)
	abstarget, err := arch.FS.Abs(target)
	if err != nil {
		return futureNode{}, false, err
	}

	filterError := func(err error) (futureNode, bool, error) {
		err = arch.error(abstarget, err)
		if err != nil {
			return futureNode{}, false, errors.WithStack(err)
		}
		return futureNode{}, true, nil
	}
	filterNotExist := func(err error) error {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	// exclude files by path before running Lstat to reduce number of lstat calls
	if !explicit && !arch.SelectByName(abstarget) {
		debug.Log("%v is excluded by path", target)
		arch.ExcludedItem(abstarget)
		return futureNode{}, true, nil
	}

	meta, err := arch.FS.OpenFile(target, fs.O_NOFOLLOW, true)
	if err != nil {
		debug.Log("open metadata for %v returned error: %v", target, err)
		// ignore if file disappeared since it was returned by readdir
		return filterError(filterNotExist(err))
	}
	closeFile := true
	defer func() {
		if closeFile {
			cerr := meta.Close()
			if err == nil {
				err = cerr
			}
		}
	}()

	// get file info and run remaining select functions that require file information
	fi, err := meta.Stat()
	if err != nil {
		debug.Log("lstat() for %v returned error: %v", target, err)
		// ignore if file disappeared since it was returned by readdir
		return filterError(filterNotExist(err))
	}
	if !arch.MandatorySelect(abstarget, fi, arch.FS) {
		debug.Log("%v is excluded by mandatory policy", target)
		arch.ExcludedItem(abstarget)
		return futureNode{}, true, nil
	}
	if !explicit && !arch.Select(abstarget, fi, arch.FS) {
		debug.Log("%v is excluded", target)
		arch.ExcludedItem(abstarget)
		return futureNode{}, true, nil
	}

	switch {
	case fi.Mode.IsRegular():
		debug.Log("  %v regular file", target)

		// check if the file has not changed before performing a fopen operation (more expensive, specially
		// in network filesystems)
		if previous != nil && arch.ReuseNode(snPath, target, fi, previous) && !fileChanged(fi, previous, arch.ChangeIgnoreFlags) {
			if arch.allBlobsPresent(previous) {
				debug.Log("%v hasn't changed, using old list of blobs", target)
				arch.trackItem(snPath, previous, previous, ItemStats{}, time.Since(start))
				arch.CompleteBlob(previous.Size)
				node, err := arch.nodeFromFileInfo(snPath, target, meta, false)
				if err != nil {
					return futureNode{}, false, err
				}

				// copy list of blobs
				node.Content = previous.Content
				arch.reconcileNode(snPath, target, node)

				fn = newFutureNodeWithResult(futureNodeResult{
					snPath: snPath,
					target: target,
					node:   node,
				})
				return fn, false, nil
			}

			debug.Log("%v hasn't changed, but contents are missing!", target)
			// There are contents missing - inform user!
			err := errors.Errorf("parts of %v not found in the repository index; storing the file again", target)
			err = arch.error(abstarget, err)
			if err != nil {
				return futureNode{}, false, err
			}
		}

		// reopen file and do an fstat() on the open file to check it is still
		// a file (and has not been exchanged for e.g. a symlink)
		err := meta.MakeReadable()
		if err != nil {
			debug.Log("MakeReadable() for %v returned error: %v", target, err)
			return filterError(err)
		}

		fi, err := meta.Stat()
		if err != nil {
			debug.Log("stat() on opened file %v returned error: %v", target, err)
			return filterError(err)
		}

		// make sure it's still a file
		if !fi.Mode.IsRegular() {
			err = errors.Errorf("file %q changed type, refusing to archive", target)
			return filterError(err)
		}

		closeFile = false

		// Save will close the file, we don't need to do that
		fn = arch.fileSaver.Save(ctx, snPath, target, meta, func() {
			arch.StartFile(snPath)
		}, func() {
			arch.trackItem(snPath, nil, nil, ItemStats{}, 0)
		}, func(node *data.Node, stats ItemStats) {
			arch.reconcileNode(snPath, target, node)
			arch.trackItem(snPath, previous, node, stats, time.Since(start))
		})

	case fi.Mode.IsDir():
		debug.Log("  %v dir", target)

		snItem := snPath + "/"
		oldSubtree, err := arch.loadSubtree(ctx, previous)
		if err != nil {
			err = arch.error(abstarget, err)
		}
		if err != nil {
			return futureNode{}, false, err
		}

		fn, err = arch.saveDir(ctx, snPath, target, meta, oldSubtree,
			func(node *data.Node, stats ItemStats) {
				arch.reconcileNode(snPath, target, node)
				arch.trackItem(snItem, previous, node, stats, time.Since(start))
			})
		if err != nil {
			debug.Log("SaveDir for %v returned error: %v", snPath, err)
			return futureNode{}, false, err
		}

	case fi.Mode&os.ModeSocket > 0:
		debug.Log("  %v is a socket, ignoring", target)
		return futureNode{}, true, nil

	default:
		debug.Log("  %v other", target)

		node, err := arch.nodeFromFileInfo(snPath, target, meta, false)
		if err != nil {
			return futureNode{}, false, err
		}
		arch.reconcileNode(snPath, target, node)
		fn = newFutureNodeWithResult(futureNodeResult{
			snPath: snPath,
			target: target,
			node:   node,
		})
	}

	debug.Log("return after %.3f", time.Since(start).Seconds())

	return fn, false, nil
}

// fileChanged tries to detect whether a file's content has changed compared
// to the contents of node, which describes the same path in the parent backup.
// It should only be run for regular files.
func fileChanged(fi *fs.ExtendedFileInfo, node *data.Node, ignoreFlags uint) bool {
	switch {
	case node == nil:
		return true
	case node.Type != data.NodeTypeFile:
		// We're only called for regular files, so this is a type change.
		return true
	case uint64(fi.Size) != node.Size:
		return true
	case !fi.ModTime.Equal(node.ModTime):
		return true
	}

	checkCtime := ignoreFlags&ChangeIgnoreCtime == 0
	checkInode := ignoreFlags&ChangeIgnoreInode == 0

	switch {
	case checkCtime && !fi.ChangeTime.Equal(node.ChangeTime):
		return true
	case checkInode && node.Inode != fi.Inode:
		return true
	}

	return false
}

// join returns all elements separated with a forward slash.
func join(elem ...string) string {
	return path.Join(elem...)
}

// saveTree stores a Tree in the repo, returned is the tree. snPath is the path
// within the current snapshot.
func (arch *Archiver) saveTree(
	ctx context.Context,
	snPath string,
	atree *tree,
	previous data.TreeNodeIterator,
	complete fileCompleteFunc,
) (futureNode, int, error) {

	var node *data.Node
	if snPath != "/" {
		if atree.FileInfoPath == "" {
			return futureNode{}, 0, errors.Errorf("FileInfoPath for %v is empty", snPath)
		}

		var err error
		node, err = arch.dirPathToNode(snPath, atree.FileInfoPath)
		if err != nil {
			return futureNode{}, 0, err
		}
	} else {
		// fake root node
		node = &data.Node{}
	}

	debug.Log("%v (%v nodes)", snPath, len(atree.Nodes))
	nodeNames := atree.NodeNames()
	nodes := make([]futureNode, 0, len(nodeNames))

	finder := data.NewTreeFinder(previous)
	defer finder.Close()

	// iterate over the nodes of atree in lexicographic (=deterministic) order
	for _, name := range nodeNames {
		if ctx.Err() != nil {
			return futureNode{}, 0, ctx.Err()
		}
		child, include, err := arch.saveTreeChild(ctx, snPath, name, atree.Nodes[name], finder)
		if err != nil {
			return futureNode{}, 0, err
		}
		if include {
			nodes = append(nodes, child)
		}
	}

	fn := arch.treeSaver.Save(ctx, snPath, atree.FileInfoPath, node, nodes, complete)
	return fn, len(nodes), nil
}

func (arch *Archiver) saveTreeChild(
	ctx context.Context,
	snapshotPath, name string,
	subtree tree,
	finder *data.TreeFinder,
) (futureNode, bool, error) {
	pathname := join(snapshotPath, name)
	oldNode, err := finder.Find(name)
	if subtree.Leaf() {
		return arch.saveTreeLeaf(ctx, pathname, subtree, oldNode, err)
	}
	itemPath := pathname + "/"
	start := time.Now()
	if err = arch.error(itemPath, err); err != nil {
		return futureNode{}, false, err
	}
	oldSubtree, err := arch.loadSubtree(ctx, oldNode)
	if err != nil {
		err = arch.error(pathname, err)
	}
	if err != nil {
		return futureNode{}, false, err
	}
	child, _, err := arch.saveTree(ctx, pathname, &subtree, oldSubtree, func(node *data.Node, stats ItemStats) {
		arch.trackItem(itemPath, oldNode, node, stats, time.Since(start))
	})
	if err != nil {
		if err = arch.error(pathname, err); err != nil {
			return futureNode{}, false, err
		}
		return futureNode{}, false, nil
	}
	return child, true, nil
}

func (arch *Archiver) saveTreeLeaf(
	ctx context.Context,
	pathname string,
	subtree tree,
	oldNode *data.Node,
	findErr error,
) (futureNode, bool, error) {
	if err := arch.error(pathname, findErr); err != nil {
		return futureNode{}, false, err
	}
	if oldNode != nil && oldNode.Type == data.NodeTypeDir && oldNode.Subtree != nil &&
		arch.ReuseSubtree(pathname, subtree.Path, oldNode) {
		node := *oldNode
		return newFutureNodeWithResult(futureNodeResult{snPath: pathname, target: subtree.Path, node: &node}), true, nil
	}
	child, excluded, err := arch.save(ctx, pathname, subtree.Path, oldNode, subtree.Explicit)
	if err != nil {
		if err = arch.error(subtree.Path, err); err != nil {
			return futureNode{}, false, err
		}
		return futureNode{}, false, nil
	}
	return child, !excluded, nil
}

func (arch *Archiver) dirPathToNode(snPath, target string) (node *data.Node, err error) {
	meta, err := arch.FS.OpenFile(target, 0, true)
	if err != nil {
		return nil, err
	}
	defer func() {
		cerr := meta.Close()
		if err == nil {
			err = cerr
		}
	}()

	debug.Log("%v, reading dir node data from %v", snPath, target)
	// in some cases reading xattrs for directories above the backup source is not allowed
	// thus ignore errors for such folders.
	node, err = arch.nodeFromFileInfo(snPath, target, meta, true)
	if err != nil {
		return nil, err
	}
	if node.Type != data.NodeTypeDir {
		return nil, errors.Errorf("path is not a directory: %v", target)
	}
	return node, err
}

// resolveRelativeTargets replaces targets that only contain relative
// directories ("." or "../../") with the contents of the directory. Each
// element of target is processed with fs.Clean().
//
// Paths returned with Explicit true are those the user listed literally; paths
// inserted from directory expansion have Explicit false.
func resolveRelativeTargets(filesys fs.FS, targets []string) ([]backupTarget, error) {
	debug.Log("targets before resolving: %v", targets)
	result := make([]backupTarget, 0, len(targets))
	for _, target := range targets {
		if target != "" && filesys.VolumeName(target) == target {
			// special case to allow users to also specify a volume name "C:" instead of a path "C:\"
			target = target + filesys.Separator()
		} else {
			target = filesys.Clean(target)
		}
		pc, _ := pathComponents(filesys, target, false)
		if len(pc) > 0 {
			result = append(result, backupTarget{Path: target, Explicit: true})
			continue
		}

		debug.Log("replacing %q with readdir(%q)", target, target)
		entries, err := fs.Readdirnames(filesys, target, fs.O_NOFOLLOW)
		if err != nil {
			return nil, err
		}
		sort.Strings(entries)

		for _, name := range entries {
			result = append(result, backupTarget{
				Path:     filesys.Join(target, name),
				Explicit: false,
			})
		}
	}

	debug.Log("targets after resolving: %v", result)
	return result, nil
}

// SnapshotOptions collect attributes for a new snapshot.
type SnapshotOptions struct {
	Tags           data.TagList
	Hostname       string
	Excludes       []string
	BackupStart    time.Time
	Time           time.Time
	ParentSnapshot *data.Snapshot
	ProgramVersion string
	// Label, Description and Delete are vaultic/rustic snapshot metadata
	// extensions (see internal/data).
	Label       string
	Description string
	Delete      *data.DeleteOption
	// SkipIfUnchanged omits the snapshot creation if it is identical to the parent snapshot.
	SkipIfUnchanged bool
	// DeferredUploader creates durable packs without publishing normal metadata.
	// Deferred crawls are always full crawls and return a null snapshot ID.
	DeferredUploader func(context.Context, func(context.Context, vaultic.BlobSaverWithAsync) error) error
}

// loadParentTree loads a tree referenced by snapshot id. If id is null, nil is returned.
func (arch *Archiver) loadParentTree(ctx context.Context, sn *data.Snapshot) data.TreeNodeIterator {
	if sn == nil {
		return nil
	}

	if sn.Tree == nil {
		debug.Log("snapshot %v has empty tree %v", *sn.ID())
		return nil
	}

	debug.Log("load parent tree %v", *sn.Tree)
	tree, err := data.LoadTree(ctx, arch.Repo, *sn.Tree)
	if err != nil {
		debug.Log("unable to load tree %v: %v", *sn.Tree, err)
		_ = arch.error("/", arch.wrapLoadTreeError(*sn.Tree, err))
		return nil
	}
	return tree
}

// runWorkers starts the worker pools, which are stopped when the context is cancelled.
func (arch *Archiver) runWorkers(ctx context.Context, wg *errgroup.Group, uploader vaultic.BlobSaverAsync) {
	arch.fileSaver = newFileSaver(ctx, wg,
		uploader,
		arch.Repo.ChunkerFactory(),
		arch.Options.ReadConcurrency)
	arch.fileSaver.CompleteBlob = arch.CompleteBlob
	arch.fileSaver.NodeFromFileInfo = arch.nodeFromFileInfo

	arch.treeSaver = newTreeSaver(ctx, wg, arch.Options.SaveTreeConcurrency, uploader, arch.Error)
}

func (arch *Archiver) stopWorkers() {
	arch.fileSaver.TriggerShutdown()
	arch.treeSaver.TriggerShutdown()
	arch.fileSaver = nil
	arch.treeSaver = nil
}

// Snapshot saves several targets and returns a snapshot.
func (arch *Archiver) Snapshot(ctx context.Context, targets []string, opts SnapshotOptions) (*data.Snapshot, vaultic.ID, *Summary, error) {
	if opts.DeferredUploader != nil && opts.ParentSnapshot != nil {
		return nil, vaultic.ID{}, nil, errors.New("deferred crawl cannot use a parent snapshot")
	}
	arch.summary = &Summary{
		BackupStart: opts.BackupStart,
	}

	cleanTargets, err := resolveRelativeTargets(arch.FS, targets)
	if err != nil {
		return nil, vaultic.ID{}, nil, err
	}

	atree, err := newTree(arch.FS, cleanTargets)
	if err != nil {
		return nil, vaultic.ID{}, nil, err
	}
	if arch.Options.CWalkConcurrency > 0 && fs.IsLocal(arch.FS) {
		queueCapacity := arch.Options.CWalkQueue
		if queueCapacity <= 0 {
			queueCapacity = 4096
		}
		var selectMu sync.Mutex
		roots := arch.Options.CWalkRoots
		if len(roots) == 0 {
			roots = targets
		}
		manifest, manifestErr := crawl.BuildDirectoryManifest(ctx, roots, arch.Options.CWalkConcurrency, queueCapacity, func(item string, _ os.FileInfo) bool {
			selectMu.Lock()
			defer selectMu.Unlock()
			if !arch.SelectByName(item) {
				return true
			}
			info, err := arch.FS.Lstat(item)
			if err != nil {
				return false
			}
			return !arch.MandatorySelect(item, info, arch.FS) || !arch.Select(item, info, arch.FS)
		})
		if manifestErr != nil {
			debug.Log("cwalk manifest unavailable, using sequential traversal: %v", manifestErr)
		} else {
			arch.cwalkManifest = manifest
			defer func() {
				_ = manifest.Close()
				arch.cwalkManifest = nil
			}()
		}
	}

	var rootTreeID vaultic.ID

	withUploader := arch.Repo.WithBlobUploader
	if opts.DeferredUploader != nil {
		withUploader = opts.DeferredUploader
	}
	err = withUploader(ctx, func(ctx context.Context, uploader vaultic.BlobSaverWithAsync) error {
		wg, wgCtx := errgroup.WithContext(ctx)
		start := time.Now()

		wg.Go(func() error {
			arch.runWorkers(wgCtx, wg, uploader)

			debug.Log("starting snapshot")
			fn, nodeCount, err := arch.saveTree(wgCtx, "/", atree, arch.loadParentTree(wgCtx, opts.ParentSnapshot), func(_ *data.Node, is ItemStats) {
				arch.trackItem("/", nil, nil, is, time.Since(start))
			})
			if err != nil {
				return err
			}

			fnr := fn.take(wgCtx)
			if fnr.err != nil {
				return fnr.err
			}

			if wgCtx.Err() != nil {
				return wgCtx.Err()
			}

			if nodeCount == 0 {
				return errors.New("snapshot is empty")
			}

			rootTreeID = *fnr.node.Subtree
			arch.stopWorkers()
			return nil
		})

		err = wg.Wait()
		debug.Log("err is %v", err)

		if err != nil {
			debug.Log("error while saving tree: %v", err)
			return err
		}
		return nil
	})
	if err != nil {
		return nil, vaultic.ID{}, nil, err
	}

	if opts.ParentSnapshot != nil && opts.SkipIfUnchanged {
		ps := opts.ParentSnapshot
		if ps.Tree != nil && rootTreeID.Equal(*ps.Tree) {
			arch.summary.BackupEnd = time.Now()
			return nil, vaultic.ID{}, arch.summary, nil
		}
	}

	sn, err := data.NewSnapshot(targets, opts.Tags, opts.Hostname, opts.Time)
	if err != nil {
		return nil, vaultic.ID{}, nil, err
	}

	sn.ProgramVersion = opts.ProgramVersion
	sn.Excludes = opts.Excludes
	sn.Label = opts.Label
	sn.Description = opts.Description
	if opts.Delete != nil && opts.Delete.IsSet() {
		sn.Delete = opts.Delete
	}
	if opts.ParentSnapshot != nil {
		sn.Parent = opts.ParentSnapshot.ID()
	}
	sn.Tree = &rootTreeID
	arch.summary.BackupEnd = time.Now()
	sn.Summary = &data.SnapshotSummary{
		BackupStart: arch.summary.BackupStart,
		BackupEnd:   arch.summary.BackupEnd,

		FilesNew:            arch.summary.Files.New,
		FilesChanged:        arch.summary.Files.Changed,
		FilesUnmodified:     arch.summary.Files.Unchanged,
		DirsNew:             arch.summary.Dirs.New,
		DirsChanged:         arch.summary.Dirs.Changed,
		DirsUnmodified:      arch.summary.Dirs.Unchanged,
		DataBlobs:           arch.summary.ItemStats.DataBlobs,
		TreeBlobs:           arch.summary.ItemStats.TreeBlobs,
		DataAdded:           arch.summary.ItemStats.DataSize + arch.summary.ItemStats.TreeSize,
		DataAddedPacked:     arch.summary.ItemStats.DataSizeInRepo + arch.summary.ItemStats.TreeSizeInRepo,
		TotalFilesProcessed: arch.summary.Files.New + arch.summary.Files.Changed + arch.summary.Files.Unchanged,
		TotalBytesProcessed: arch.summary.ProcessedBytes,
	}

	if opts.DeferredUploader != nil {
		return sn, vaultic.ID{}, arch.summary, nil
	}
	if err := arch.BeforeSnapshot(); err != nil {
		return nil, vaultic.ID{}, nil, err
	}
	id, err := data.SaveSnapshot(ctx, arch.Repo, sn)
	if err != nil {
		return nil, vaultic.ID{}, nil, err
	}

	return sn, id, arch.summary, nil
}
