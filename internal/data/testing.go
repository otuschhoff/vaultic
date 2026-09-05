package data

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"slices"
	"strings"
	"testing"
	"time"

	rtest "github.com/otuschhoff/vaultic/internal/test"
	"github.com/otuschhoff/vaultic/internal/vaultic"
	"github.com/restic/chunker"
)

// fakeFile returns a reader which yields deterministic pseudo-random data.
func fakeFile(seed, size int64) io.Reader {
	return io.LimitReader(rand.New(rand.NewSource(seed)), size)
}

type fakeFileSystem struct {
	t       testing.TB
	repo    vaultic.Repository
	buf     []byte
	chunker *chunker.Chunker
	rand    *rand.Rand
}

// saveFile reads from rd and saves the blobs in the repository. The list of
// IDs is returned.
func (fs *fakeFileSystem) saveFile(ctx context.Context, uploader vaultic.BlobSaver, rd io.Reader) (blobs vaultic.IDs) {
	if fs.buf == nil {
		fs.buf = make([]byte, chunker.MaxSize)
	}

	if fs.chunker == nil {
		fs.chunker = chunker.New(rd, fs.repo.Config().ChunkerPolynomial)
	} else {
		fs.chunker.Reset(rd, fs.repo.Config().ChunkerPolynomial)
	}

	blobs = vaultic.IDs{}
	for {
		chunk, err := fs.chunker.Next(fs.buf)
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			fs.t.Fatalf("unable to save chunk in repo: %v", err)
		}

		id, _, _, err := uploader.SaveBlob(ctx, vaultic.DataBlob, chunk.Data, vaultic.ID{}, false)
		if err != nil {
			fs.t.Fatalf("error saving chunk: %v", err)
		}

		blobs = append(blobs, id)
	}

	return blobs
}

const (
	maxFileSize = 20000
	maxSeed     = 32
	maxNodes    = 15
)

// saveTree saves a tree of fake files in the repo and returns the ID.
func (fs *fakeFileSystem) saveTree(ctx context.Context, uploader vaultic.BlobSaver, seed int64, depth int) vaultic.ID {
	rnd := rand.NewSource(seed)
	numNodes := int(rnd.Int63() % maxNodes)

	var nodes []*Node
	for i := range numNodes {
		// randomly select the type of the node, either tree (p = 1/4) or file (p = 3/4).
		if depth > 1 && rnd.Int63()%4 == 0 {
			treeSeed := rnd.Int63() % maxSeed
			id := fs.saveTree(ctx, uploader, treeSeed, depth-1)

			node := &Node{
				Name:    fmt.Sprintf("dir-%v", i),
				Type:    NodeTypeDir,
				Mode:    0755,
				Subtree: &id,
			}

			nodes = append(nodes, node)
			continue
		}

		fileSeed := rnd.Int63() % maxSeed
		fileSize := (maxFileSize / maxSeed) * fileSeed

		node := &Node{
			Name: fmt.Sprintf("file-%v", i),
			Type: NodeTypeFile,
			Mode: 0644,
			Size: uint64(fileSize),
		}

		node.Content = fs.saveFile(ctx, uploader, fakeFile(fileSeed, fileSize))
		nodes = append(nodes, node)
	}

	return TestSaveNodes(fs.t, ctx, uploader, nodes)
}

//nolint:revive // as this is a test helper, t should go first
func TestSaveNodes(t testing.TB, ctx context.Context, uploader vaultic.BlobSaver, nodes []*Node) vaultic.ID {
	slices.SortFunc(nodes, func(a, b *Node) int {
		return strings.Compare(a.Name, b.Name)
	})
	treeWriter := NewTreeWriter(uploader)
	for _, node := range nodes {
		err := treeWriter.AddNode(node)
		rtest.OK(t, err)
	}
	id, err := treeWriter.Finalize(ctx)
	rtest.OK(t, err)
	return id
}

// TestCreateSnapshot creates a snapshot filled with fake data. The
// fake data is generated deterministically from the timestamp `at`, which is
// also used as the snapshot's timestamp. The tree's depth can be specified
// with the parameter depth. The parameter duplication is a probability that
// the same blob will saved again.
func TestCreateSnapshot(t testing.TB, repo vaultic.Repository, at time.Time, depth int) *Snapshot {
	seed := at.Unix()
	t.Logf("create fake snapshot at %s with seed %d", at, seed)

	fakedir := fmt.Sprintf("fakedir-at-%v", at.Format("2006-01-02 15:04:05"))
	snapshot, err := NewSnapshot([]string{fakedir}, []string{"test"}, "foo", at)
	if err != nil {
		t.Fatal(err)
	}

	fs := fakeFileSystem{
		t:    t,
		repo: repo,
		rand: rand.New(rand.NewSource(seed)),
	}

	var treeID vaultic.ID
	rtest.OK(t, repo.WithBlobUploader(context.TODO(), func(ctx context.Context, uploader vaultic.BlobSaverWithAsync) error {
		treeID = fs.saveTree(ctx, uploader, seed, depth)
		return nil
	}))
	snapshot.Tree = &treeID

	id, err := SaveSnapshot(context.TODO(), repo, snapshot)
	if err != nil {
		t.Fatal(err)
	}

	snapshot.id = &id

	t.Logf("saved snapshot %v", id.Str())

	return snapshot
}

// TestSetSnapshotID sets the snapshot's ID.
func TestSetSnapshotID(_ testing.TB, sn *Snapshot, id vaultic.ID) {
	sn.id = &id
}

// ParseDurationOrPanic parses a duration from a string or panics if string is invalid.
// The format is `6y5m234d37h`.
func ParseDurationOrPanic(s string) Duration {
	d, err := ParseDuration(s)
	if err != nil {
		//nolint:forbidigo // This existing panic enforces an internal invariant; new panic paths remain forbidden.
		panic(err)
	}

	return d
}

// TestLoadAllSnapshots returns a list of all snapshots in the repo.
// If a snapshot ID is in excludeIDs, it will not be included in the result.
func TestLoadAllSnapshots(ctx context.Context, repo vaultic.ListerLoaderUnpacked, excludeIDs vaultic.IDSet) (snapshots Snapshots, err error) {
	err = ForAllSnapshots(ctx, repo, repo, excludeIDs, func(id vaultic.ID, sn *Snapshot, err error) error {
		if err != nil {
			return err
		}

		snapshots = append(snapshots, sn)
		return nil
	})

	if err != nil {
		return nil, err
	}

	return snapshots, nil
}

// TestTreeMap returns the trees from the map on LoadTree.
type TestTreeMap map[vaultic.ID][]byte

func (t TestTreeMap) LoadBlob(_ context.Context, bh vaultic.BlobHandle, _ []byte) ([]byte, error) {
	if bh.Type != vaultic.TreeBlob {
		return nil, fmt.Errorf("can only load trees")
	}
	tree, ok := t[bh.ID]
	if !ok {
		return nil, fmt.Errorf("tree not found")
	}
	return tree, nil
}

func (t TestTreeMap) Connections() uint {
	return 2
}

// TestWritableTreeMap also support saving
type TestWritableTreeMap struct {
	TestTreeMap
}

func (t TestWritableTreeMap) SaveBlob(
	_ context.Context,
	tpe vaultic.BlobType,
	buf []byte,
	id vaultic.ID,
	_ bool,
) (newID vaultic.ID, known bool, size int, err error) {
	if tpe != vaultic.TreeBlob {
		return vaultic.ID{}, false, 0, fmt.Errorf("can only save trees")
	}

	if id.IsNull() {
		id = vaultic.Hash(buf)
	}
	_, ok := t.TestTreeMap[id]
	if ok {
		return id, false, 0, nil
	}

	t.TestTreeMap[id] = append([]byte{}, buf...)
	return id, true, len(buf), nil
}

func (t TestWritableTreeMap) Dump(test testing.TB) {
	for k, v := range t.TestTreeMap {
		test.Logf("%v: %v", k, string(v))
	}
}
