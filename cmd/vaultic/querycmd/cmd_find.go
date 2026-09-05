package querycmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/debug"
	"github.com/otuschhoff/vaultic/internal/errors"
	"github.com/otuschhoff/vaultic/internal/filter"
	"github.com/otuschhoff/vaultic/internal/global"
	"github.com/otuschhoff/vaultic/internal/ui"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
	"github.com/otuschhoff/vaultic/internal/vaultic"
	"github.com/otuschhoff/vaultic/internal/walker"
)

func NewFindCommand(globalOptions *global.Options) *cobra.Command {
	var options findOptions

	cmd := &cobra.Command{
		Use:   "find [flags] PATTERN...",
		Short: "Find a file, a directory or vaultic IDs",
		Long: `
The "find" command searches for files or directories in snapshots stored in the
repository. It can also be used to search for vaultic blobs, trees or pack
files for troubleshooting.

The default sort option for the snapshots is youngest to oldest. To sort the
output from oldest to youngest specify --reverse.

EXIT STATUS
===========

Exit status is 0 if the command was successful.
Exit status is 1 if there was any error.
Exit status is 10 if the repository does not exist.
Exit status is 11 if the repository is already locked.
Exit status is 12 if the password is incorrect.
`,
		Example: `vaultic find config.json
vaultic find --json "*.yml" "*.json"
vaultic find --json --blob 420f620f b46ebe8a ddd38656
vaultic find --show-pack-id --blob 420f620f
vaultic find --tree 577c2bc9 f81f2e22 a62827a9
vaultic find --pack 025c1d06`,
		GroupID:           "default",
		DisableAutoGenTag: true,
		PreRunE: func(_ *cobra.Command, _ []string) error {
			finalizeSnapshotFilter(&options.SnapshotFilter)
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return runFind(cmd.Context(), options, *globalOptions, args, globalOptions.Term)
		},
	}

	options.AddFlags(cmd.Flags())
	return cmd
}

// findOptions bundles all options for the find command.
type findOptions struct {
	Oldest             string
	Newest             string
	Snapshots          []string
	BlobID, TreeID     bool
	PackID, ShowPackID bool
	CaseInsensitive    bool
	ListLong           bool
	HumanReadable      bool
	Reverse            bool
	data.SnapshotFilter
}

func (options *findOptions) AddFlags(f *pflag.FlagSet) {
	f.StringVarP(&options.Oldest, "oldest", "O", "", "oldest modification date/time")
	f.StringVarP(&options.Newest, "newest", "N", "", "newest modification date/time")
	f.StringArrayVarP(&options.Snapshots, "snapshot", "s", nil, "snapshot `id` to search in (can be given multiple times)")
	f.BoolVar(&options.BlobID, "blob", false, "pattern is a blob-ID")
	f.BoolVar(&options.TreeID, "tree", false, "pattern is a tree-ID")
	f.BoolVar(&options.PackID, "pack", false, "pattern is a pack-ID")
	f.BoolVar(&options.ShowPackID, "show-pack-id", false, "display the pack-ID the blobs belong to (with --blob or --tree)")
	f.BoolVarP(&options.CaseInsensitive, "ignore-case", "i", false, "ignore case for pattern")
	f.BoolVarP(&options.Reverse, "reverse", "R", false, "reverse sort order oldest to newest")
	f.BoolVarP(&options.ListLong, "long", "l", false, "use a long listing format showing size and mode")
	f.BoolVar(&options.HumanReadable, "human-readable", false, "print sizes in human readable format")

	initMultiSnapshotFilter(f, &options.SnapshotFilter, true)
}

type findPattern struct {
	oldest, newest time.Time
	pattern        []string
	ignoreCase     bool
}

var timeFormats = []string{
	"2006-01-02",
	"2006-01-02 15:04",
	"2006-01-02 15:04:05",
	"2006-01-02 15:04:05 -0700",
	"2006-01-02 15:04:05 MST",
	"02.01.2006",
	"02.01.2006 15:04",
	"02.01.2006 15:04:05",
	"02.01.2006 15:04:05 -0700",
	"02.01.2006 15:04:05 MST",
	"Mon Jan 2 15:04:05 -0700 MST 2006",
}

func parseTime(str string) (time.Time, error) {
	for _, fmt := range timeFormats {
		if t, err := time.ParseInLocation(fmt, str, time.Local); err == nil {
			return t, nil
		}
	}

	return time.Time{}, errors.Fatalf("unable to parse time: %q", str)
}

type statefulOutput struct {
	ListLong      bool
	HumanReadable bool
	JSON          bool
	inuse         bool
	newsn         *data.Snapshot
	oldsn         *data.Snapshot
	hits          int
	printer       interface {
		S(string, ...any)
		P(string, ...any)
		E(string, ...any)
	}
	stdout io.Writer
	err    error
}

func (s *statefulOutput) write(data []byte) {
	if s.err == nil {
		_, s.err = s.stdout.Write(data)
	}
}

func (s *statefulOutput) printf(format string, args ...any) {
	if s.err == nil {
		_, s.err = fmt.Fprintf(s.stdout, format, args...)
	}
}

func (s *statefulOutput) PrintPatternJSON(path string, node *data.Node) {
	type findNode data.Node
	b, err := json.Marshal(struct {
		// Add these attributes
		Path        string `json:"path,omitempty"`
		Permissions string `json:"permissions,omitempty"`

		*findNode

		// Make the following attributes disappear
		Name               byte `json:"name,omitempty"`
		ExtendedAttributes byte `json:"extended_attributes,omitempty"`
		GenericAttributes  byte `json:"generic_attributes,omitempty"`
		Device             byte `json:"device,omitempty"`
		Content            byte `json:"content,omitempty"`
		Subtree            byte `json:"subtree,omitempty"`
	}{
		Path:        path,
		Permissions: node.Mode.String(),
		findNode:    (*findNode)(node),
	})
	if err != nil {
		s.printer.E("Marshal failed: %v", err)
		return
	}
	if !s.inuse {
		s.write([]byte("["))
		s.inuse = true
	}
	if s.newsn != s.oldsn {
		if s.oldsn != nil {
			s.printf("],\"hits\":%d,\"snapshot\":%q},", s.hits, s.oldsn.ID())
		}
		s.write([]byte(`{"matches":[`))
		s.oldsn = s.newsn
		s.hits = 0
	}
	if s.hits > 0 {
		s.write([]byte(","))
	}
	s.write(b)
	s.hits++
}

func (s *statefulOutput) PrintPatternNormal(path string, node *data.Node) {
	if s.newsn != s.oldsn {
		if s.oldsn != nil {
			s.printer.P("")
		}
		s.oldsn = s.newsn
		s.printer.P("Found matching entries in snapshot %s from %s", s.oldsn.ID().Str(), s.oldsn.Time.Local().Format(global.TimeFormat))
	}
	s.printer.S(formatNode(path, node, s.ListLong, s.HumanReadable))
}

func (s *statefulOutput) PrintPattern(path string, node *data.Node) {
	if s.JSON {
		s.PrintPatternJSON(path, node)
	} else {
		s.PrintPatternNormal(path, node)
	}
}

func (s *statefulOutput) PrintObjectJSON(kind, id, nodepath, treeID string, sn *data.Snapshot) {
	b, err := json.Marshal(struct {
		// Add these attributes
		ObjectType string    `json:"object_type"`
		ID         string    `json:"id"`
		Path       string    `json:"path"`
		ParentTree string    `json:"parent_tree,omitempty"`
		SnapshotID string    `json:"snapshot"`
		Time       time.Time `json:"time"`
	}{
		ObjectType: kind,
		ID:         id,
		Path:       nodepath,
		SnapshotID: sn.ID().String(),
		ParentTree: treeID,
		Time:       sn.Time,
	})
	if err != nil {
		s.printer.E("Marshal failed: %v", err)
		return
	}
	if !s.inuse {
		s.write([]byte("["))
		s.inuse = true
	}
	if s.hits > 0 {
		s.write([]byte(","))
	}
	s.write(b)
	s.hits++
}

func (s *statefulOutput) PrintObjectNormal(kind, id, nodepath, treeID string, sn *data.Snapshot) {
	s.printer.S("Found %s %s", kind, id)
	if kind == "blob" {
		s.printer.S(" ... in file %s", nodepath)
		s.printer.S("     (tree %s)", treeID)
	} else {
		s.printer.S(" ... path %s", nodepath)
	}
	s.printer.S(" ... in snapshot %s (%s)", sn.ID().Str(), sn.Time.Local().Format(global.TimeFormat))
}

func (s *statefulOutput) PrintObject(kind, id, nodepath, treeID string, sn *data.Snapshot) {
	if s.JSON {
		s.PrintObjectJSON(kind, id, nodepath, treeID, sn)
	} else {
		s.PrintObjectNormal(kind, id, nodepath, treeID, sn)
	}
}

func (s *statefulOutput) Finish() error {
	if s.JSON {
		// do some finishing up
		if s.oldsn != nil {
			s.printf("],\"hits\":%d,\"snapshot\":%q}", s.hits, s.oldsn.ID())
		}
		if s.inuse {
			s.write([]byte("]\n"))
		} else {
			s.write([]byte("[]\n"))
		}
	}
	return s.err
}

// Finder bundles information needed to find a file or directory.
type Finder struct {
	repo       vaultic.Repository
	pat        findPattern
	out        statefulOutput
	blobIDs    map[string]struct{}
	treeIDs    map[string]struct{}
	itemsFound int
	printer    interface {
		S(string, ...any)
		P(string, ...any)
		E(string, ...any)
	}
}

func (f *Finder) findInSnapshot(ctx context.Context, sn *data.Snapshot) error {
	debug.Log("searching in snapshot %s\n  for entries within [%s %s]", sn.ID(), f.pat.oldest, f.pat.newest)

	if sn.Tree == nil {
		return errors.Errorf("snapshot %v has no tree", sn.ID().Str())
	}

	f.out.newsn = sn
	return walker.Walk(ctx, f.repo, *sn.Tree, walker.WalkVisitor{ProcessNode: f.findPatternNode(sn)})
}

func (f *Finder) findPatternNode(snapshot *data.Snapshot) walker.WalkFunc {
	return func(parentTreeID vaultic.ID, nodepath string, node *data.Node, nodeErr error) error {
		if nodeErr != nil {
			debug.Log("Error loading tree %v: %v", parentTreeID, nodeErr)
			f.printer.S("Unable to load tree %s", parentTreeID)
			f.printer.S(" ... which belongs to snapshot %s", snapshot.ID())
			return walker.ErrSkipNode
		}
		if node == nil {
			return nil
		}
		normalizedPath := nodepath
		if f.pat.ignoreCase {
			normalizedPath = strings.ToLower(nodepath)
		}
		found, err := matchesAnyPattern(f.pat.pattern, normalizedPath)
		if err != nil {
			return err
		}
		errIfNoMatch, err := f.childMismatch(node, normalizedPath)
		if err != nil || !found {
			return errors.Join(err, errIfNoMatch)
		}
		if !f.pat.oldest.IsZero() && node.ModTime.Before(f.pat.oldest) {
			debug.Log("    ModTime is older than %s\n", f.pat.oldest)
			return errIfNoMatch
		}
		if !f.pat.newest.IsZero() && node.ModTime.After(f.pat.newest) {
			debug.Log("    ModTime is newer than %s\n", f.pat.newest)
			return errIfNoMatch
		}
		debug.Log("    found match\n")
		f.out.PrintPattern(nodepath, node)
		return nil
	}
}

func matchesAnyPattern(patterns []string, path string) (bool, error) {
	for _, pattern := range patterns {
		matched, err := filter.Match(pattern, path)
		if err != nil || matched {
			return matched, err
		}
	}
	return false, nil
}

func (f *Finder) childMismatch(node *data.Node, path string) (error, error) {
	if node.Type != data.NodeTypeDir {
		return nil, nil
	}
	for _, pattern := range f.pat.pattern {
		mayMatch, err := filter.ChildMatch(pattern, path)
		if err != nil {
			return nil, err
		}
		if mayMatch {
			return nil, nil
		}
	}
	return walker.ErrSkipNode, nil
}

func (f *Finder) findTree(treeID vaultic.ID, nodepath string) error {
	found := false
	if _, ok := f.treeIDs[treeID.String()]; ok {
		found = true
	} else if _, ok := f.treeIDs[treeID.Str()]; ok {
		found = true
	}
	if found {
		f.out.PrintObject("tree", treeID.String(), nodepath, "", f.out.newsn)
		f.itemsFound++
		// Terminate if we have found all trees (and we are not
		// looking for blobs)
		if f.itemsFound >= len(f.treeIDs) && len(f.blobIDs) == 0 {
			// Return an error to terminate the Walk
			return errors.ErrStopIteration
		}
	}
	return nil
}

func (f *Finder) findIDs(ctx context.Context, sn *data.Snapshot) error {
	debug.Log("searching IDs in snapshot %s", sn.ID())

	if sn.Tree == nil {
		return errors.Errorf("snapshot %v has no tree", sn.ID().Str())
	}

	f.out.newsn = sn
	return walker.Walk(ctx, f.repo, *sn.Tree, walker.WalkVisitor{ProcessNode: f.findIDNode(ctx, sn)})
}

func (f *Finder) findIDNode(ctx context.Context, snapshot *data.Snapshot) walker.WalkFunc {
	return func(parentTreeID vaultic.ID, nodepath string, node *data.Node, nodeErr error) error {
		if nodeErr != nil {
			debug.Log("Error loading tree %v: %v", parentTreeID, nodeErr)
			f.printer.S("Unable to load tree %s", parentTreeID)
			f.printer.S(" ... which belongs to snapshot %s", snapshot.ID())
			return walker.ErrSkipNode
		}
		if node == nil {
			if nodepath == "/" {
				return f.findTree(parentTreeID, "/")
			}
			return nil
		}
		if node.Type == data.NodeTypeDir && len(f.treeIDs) > 0 {
			if err := f.findTree(*node.Subtree, nodepath); err != nil {
				return err
			}
		}
		if node.Type != data.NodeTypeFile || len(f.blobIDs) == 0 {
			return nil
		}
		return f.findNodeBlobs(ctx, snapshot, parentTreeID, nodepath, node.Content)
	}
}

func (f *Finder) findNodeBlobs(ctx context.Context, snapshot *data.Snapshot, parentTreeID vaultic.ID, nodepath string, ids vaultic.IDs) error {
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return err
		}
		idString := id.String()
		if _, ok := f.blobIDs[idString]; !ok {
			if _, ok := f.blobIDs[id.Str()]; !ok {
				continue
			}
			f.blobIDs[idString] = struct{}{}
			delete(f.blobIDs, id.Str())
		}
		f.out.PrintObject("blob", idString, nodepath, parentTreeID.String(), snapshot)
	}
	return nil
}

func (f *Finder) addBlobHandle(h vaultic.BlobHandle) error {
	switch h.Type {
	case vaultic.DataBlob:
		f.blobIDs[h.ID.String()] = struct{}{}
	case vaultic.TreeBlob:
		f.treeIDs[h.ID.String()] = struct{}{}
	default:
		return fmt.Errorf("unknown type %v in blob list", h.Type.String())
	}
	return nil
}

// packsToBlobs converts the list of pack IDs to a list of blob IDs that
// belong to those packs.
func (f *Finder) packsToBlobs(ctx context.Context, packs []string) error {
	packIDs := make(map[string]struct{})
	for _, p := range packs {
		packIDs[p] = struct{}{}
	}
	if f.blobIDs == nil {
		f.blobIDs = make(map[string]struct{})
	}
	if f.treeIDs == nil {
		f.treeIDs = make(map[string]struct{})
	}

	debug.Log("Looking for packs...")
	err := f.repo.List(ctx, vaultic.PackFile, func(id vaultic.ID, size int64) error {
		idStr := id.String()
		if _, ok := packIDs[idStr]; !ok {
			// Look for short ID form
			if _, ok := packIDs[id.Str()]; !ok {
				return nil
			}
			delete(packIDs, id.Str())
			packIDs[idStr] = struct{}{}
		}
		debug.Log("Found pack %s", idStr)
		handles, err := f.repo.ListPackHandles(ctx, id, size)
		if err != nil {
			// ignore error to allow fallback to index
			return nil
		}
		for _, h := range handles {
			if err := f.addBlobHandle(h); err != nil {
				return err
			}
		}
		// forget successfully processed pack
		delete(packIDs, idStr)
		// Stop searching when all packs have been found
		if len(packIDs) == 0 {
			return errors.ErrStopIteration
		}
		return nil
	})
	if err != nil && !errors.Is(err, errors.ErrStopIteration) {
		return err
	}

	if len(packIDs) > 0 {
		// try to resolve unknown pack ids from the index
		packIDs, err = f.indexPacksToBlobs(ctx, packIDs)
		if err != nil {
			return err
		}
	}

	if len(packIDs) > 0 {
		list := make([]string, 0, len(packIDs))
		for h := range packIDs {
			list = append(list, h)
		}

		sort.Strings(list)
		return errors.Fatalf("unable to find pack(s): %v", list)
	}

	debug.Log("%d blobs %v trees found", len(f.blobIDs), len(f.treeIDs))
	return nil
}

func (f *Finder) indexPacksToBlobs(ctx context.Context, packIDs map[string]struct{}) (map[string]struct{}, error) {
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// remember which packs were found in the index
	indexPackIDs := make(map[string]struct{})
	var callbackErr error
	err := f.repo.ListBlobs(wctx, func(pb vaultic.PackBlob) {
		if callbackErr != nil {
			return
		}
		packID := pb.PackID()
		idStr := packID.String()
		// keep entry in packIDs as Each() returns individual index entries
		matchingID := false
		if _, ok := packIDs[idStr]; ok {
			matchingID = true
		} else {
			if _, ok := packIDs[packID.Str()]; ok {
				// expand id
				delete(packIDs, packID.Str())
				packIDs[idStr] = struct{}{}
				matchingID = true
			}
		}
		if matchingID {
			callbackErr = f.addBlobHandle(pb.Handle())
			if callbackErr != nil {
				cancel()
				return
			}
			indexPackIDs[idStr] = struct{}{}
		}
	})
	if err != nil {
		return nil, err
	}
	if callbackErr != nil {
		return nil, callbackErr
	}

	for id := range indexPackIDs {
		delete(packIDs, id)
	}

	return packIDs, nil
}

func (f *Finder) findObjectPack(id string, t vaultic.BlobType) {
	rid, err := vaultic.ParseID(id)
	if err != nil {
		f.printer.S("Note: cannot find pack for object '%s', unable to parse ID: %v", id, err)
		return
	}

	blobs := f.repo.LookupBlob(vaultic.BlobHandle{Type: t, ID: rid})
	if len(blobs) == 0 {
		f.printer.S("Object %s with type %s not found in the index", t.String(), rid.Str())
		return
	}

	for _, b := range blobs {
		if b.Handle().ID.Equal(rid) {
			f.printer.S("Object belongs to pack %s", b.PackID())
			f.printer.S(" ... Pack %s: %v", b.PackID().String(), b.Handle())
			break
		}
	}
}

func (f *Finder) findObjectsPacks() {
	for i := range f.blobIDs {
		f.findObjectPack(i, vaultic.DataBlob)
	}

	for i := range f.treeIDs {
		f.findObjectPack(i, vaultic.TreeBlob)
	}
}

func parseFindPattern(options findOptions, args []string) (findPattern, error) {
	var err error
	pat := findPattern{pattern: args}
	if options.CaseInsensitive {
		for i := range pat.pattern {
			pat.pattern[i] = strings.ToLower(pat.pattern[i])
		}
		pat.ignoreCase = true
	}

	if options.Oldest != "" {
		if pat.oldest, err = parseTime(options.Oldest); err != nil {
			return findPattern{}, err
		}
	}

	if options.Newest != "" {
		if pat.newest, err = parseTime(options.Newest); err != nil {
			return findPattern{}, err
		}
	}

	if !pat.newest.IsZero() && !pat.oldest.IsZero() && pat.oldest.After(pat.newest) {
		return findPattern{}, errors.Fatal("--oldest must specify a time before --newest")
	}
	return pat, nil
}

func validateFindIDOptions(options findOptions) error {
	if (options.BlobID && options.TreeID) ||
		(options.BlobID && options.PackID) ||
		(options.TreeID && options.PackID) {
		return errors.Fatal("cannot have several ID types")
	}
	return nil
}

func (f *Finder) prepareIDSearch(ctx context.Context, options findOptions) error {
	if options.BlobID {
		f.blobIDs = make(map[string]struct{}, len(f.pat.pattern))
		for _, pattern := range f.pat.pattern {
			f.blobIDs[pattern] = struct{}{}
		}
	}
	if options.TreeID {
		f.treeIDs = make(map[string]struct{}, len(f.pat.pattern))
		for _, pattern := range f.pat.pattern {
			f.treeIDs[pattern] = struct{}{}
		}
	}
	if options.PackID {
		return f.packsToBlobs(ctx, f.pat.pattern)
	}
	return nil
}

func (f *Finder) searchSnapshots(ctx context.Context, snapshots []*data.Snapshot) error {
	for _, snapshot := range snapshots {
		if len(f.blobIDs) > 0 || len(f.treeIDs) > 0 {
			if err := f.findIDs(ctx, snapshot); err != nil && !errors.Is(err, errors.ErrStopIteration) {
				return err
			}
			continue
		}
		if err := f.findInSnapshot(ctx, snapshot); err != nil {
			return err
		}
	}
	return nil
}

func runFind(ctx context.Context, options findOptions, globalOptions global.Options, args []string, term ui.Terminal) error {
	if len(args) == 0 {
		return errors.Fatal("wrong number of arguments")
	}
	if err := validateFindIDOptions(options); err != nil {
		return err
	}
	pat, err := parseFindPattern(options, args)
	if err != nil {
		return err
	}
	printer := progress.NewTerminalPrinter(globalOptions.JSON, globalOptions.Verbosity, term)

	ctx, repo, unlock, err := openWithReadLock(ctx, globalOptions, globalOptions.NoLock, printer)
	if err != nil {
		return err
	}
	defer unlock()

	snapshotLister, err := vaultic.MemorizeList(ctx, repo, vaultic.SnapshotFile)
	if err != nil {
		return err
	}
	if err = repo.LoadIndex(ctx, printer); err != nil {
		return err
	}

	f := &Finder{
		repo: repo,
		pat:  pat,
		out: statefulOutput{
			ListLong: options.ListLong, HumanReadable: options.HumanReadable,
			JSON: globalOptions.JSON, printer: printer, stdout: term.OutputRaw(),
		},
		printer: printer,
	}
	if err := f.prepareIDSearch(ctx, options); err != nil {
		return err
	}

	var filteredSnapshots []*data.Snapshot
	err = options.SnapshotFilter.FindAll(ctx, snapshotLister, repo, options.Snapshots, func(_ string, sn *data.Snapshot, err error) error {
		if err != nil {
			return err
		}
		filteredSnapshots = append(filteredSnapshots, sn)
		return nil
	})
	if err != nil {
		return err
	}

	sort.Slice(filteredSnapshots, func(i, j int) bool {
		if options.Reverse {
			return filteredSnapshots[i].Time.Before(filteredSnapshots[j].Time)
		}
		return filteredSnapshots[i].Time.After(filteredSnapshots[j].Time)
	})

	if err := f.searchSnapshots(ctx, filteredSnapshots); err != nil {
		return err
	}
	if err := f.out.Finish(); err != nil {
		return fmt.Errorf("write find output: %w", err)
	}

	if options.ShowPackID && (len(f.blobIDs) > 0 || len(f.treeIDs) > 0) {
		f.findObjectsPacks()
	}

	return nil
}
