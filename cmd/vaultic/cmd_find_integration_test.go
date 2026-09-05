package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/backend"
	"github.com/otuschhoff/vaultic/internal/global"
	rtest "github.com/otuschhoff/vaultic/internal/test"
	"github.com/otuschhoff/vaultic/internal/ui/progress"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func testRunFind(t testing.TB, wantJSON bool, options findOptions, globalOptions global.Options, pattern string) []byte {
	buf, err := withCaptureStdout(t, globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		globalOptions.JSON = wantJSON

		return runFind(ctx, options, globalOptions, []string{pattern}, globalOptions.Term)
	})
	rtest.OK(t, err)
	return buf.Bytes()
}

func TestFind(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	datafile := testSetupBackupData(t, env)
	options := backupOptions{}

	testRunBackup(t, "", []string{env.testdata}, options, env.globalOptions)

	results := testRunFind(t, false, findOptions{}, env.globalOptions, "unexistingfile")
	rtest.Assert(t, len(results) == 0, "unexisting file found in repo (%v)", datafile)

	results = testRunFind(t, false, findOptions{}, env.globalOptions, "testfile")
	lines := strings.Split(string(results), "\n")
	rtest.Assert(t, len(lines) == 2, "expected one file found in repo (%v)", datafile)

	results = testRunFind(t, false, findOptions{}, env.globalOptions, "testfile*")
	lines = strings.Split(string(results), "\n")
	rtest.Assert(t, len(lines) == 4, "expected three files found in repo (%v)", datafile)
}

type testMatch struct {
	Path        string    `json:"path,omitempty"`
	Permissions string    `json:"permissions,omitempty"`
	Size        uint64    `json:"size,omitempty"`
	Date        time.Time `json:"date"`
	UID         uint32    `json:"uid,omitempty"`
	GID         uint32    `json:"gid,omitempty"`
}

type testMatches struct {
	Hits       int         `json:"hits,omitempty"`
	SnapshotID string      `json:"snapshot,omitempty"`
	Matches    []testMatch `json:"matches,omitempty"`
}

func TestFindJSON(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	datafile := testSetupBackupData(t, env)
	options := backupOptions{}

	testRunBackup(t, "", []string{env.testdata}, options, env.globalOptions)
	testRunCheck(t, env.globalOptions)
	snapshot, _ := testRunSnapshots(t, env.globalOptions)

	results := testRunFind(t, true, findOptions{}, env.globalOptions, "unexistingfile")
	matches := []testMatches{}
	rtest.OK(t, json.Unmarshal(results, &matches))
	rtest.Assert(t, len(matches) == 0, "expected no match in repo (%v)", datafile)

	results = testRunFind(t, true, findOptions{}, env.globalOptions, "testfile")
	rtest.OK(t, json.Unmarshal(results, &matches))
	rtest.Assert(t, len(matches) == 1, "expected a single snapshot in repo (%v)", datafile)
	rtest.Assert(t, len(matches[0].Matches) == 1, "expected a single file to match (%v)", datafile)
	rtest.Assert(t, matches[0].Hits == 1, "expected hits to show 1 match (%v)", datafile)

	results = testRunFind(t, true, findOptions{}, env.globalOptions, "testfile*")
	rtest.OK(t, json.Unmarshal(results, &matches))
	rtest.Assert(t, len(matches) == 1, "expected a single snapshot in repo (%v)", datafile)
	rtest.Assert(t, len(matches[0].Matches) == 3, "expected 3 files to match (%v)", datafile)
	rtest.Assert(t, matches[0].Hits == 3, "expected hits to show 3 matches (%v)", datafile)

	results = testRunFind(t, true, findOptions{TreeID: true}, env.globalOptions, snapshot.Tree.String())
	rtest.OK(t, json.Unmarshal(results, &matches))
	rtest.Assert(t, len(matches) == 1, "expected a single snapshot in repo (%v)", matches)
	rtest.Assert(t, len(matches[0].Matches) == 3, "expected 3 files to match (%v)", matches[0].Matches)
	rtest.Assert(t, matches[0].Hits == 3, "expected hits to show 3 matches (%v)", datafile)
}

func TestFindSorting(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	testSetupBackupData(t, env)
	options := backupOptions{}

	// first backup
	testRunBackup(t, "", []string{env.testdata}, options, env.globalOptions)
	sn1 := testListSnapshots(t, env.globalOptions, 1)[0]

	// second backup
	testRunBackup(t, "", []string{env.testdata}, options, env.globalOptions)
	snapshots := testListSnapshots(t, env.globalOptions, 2)
	// get id of new snapshot without depending on file order returned by filesystem
	sn2 := snapshots[0]
	if sn1.Equal(sn2) {
		sn2 = snapshots[1]
	}

	// first vaultic find - with default FindOptions{}
	results := testRunFind(t, true, findOptions{}, env.globalOptions, "testfile")
	lines := strings.Split(string(results), "\n")
	rtest.Assert(t, len(lines) == 2, "expected two lines of output, found %d", len(lines))
	matches := []testMatches{}
	rtest.OK(t, json.Unmarshal(results, &matches))

	// run second vaultic find with --reverse, sort oldest to newest
	resultsReverse := testRunFind(t, true, findOptions{Reverse: true}, env.globalOptions, "testfile")
	lines = strings.Split(string(resultsReverse), "\n")
	rtest.Assert(t, len(lines) == 2, "expected two lines of output, found %d", len(lines))
	matchesReverse := []testMatches{}
	rtest.OK(t, json.Unmarshal(resultsReverse, &matchesReverse))

	// compare result sets
	rtest.Assert(t, sn1.String() == matchesReverse[0].SnapshotID, "snapshot[0] must match old snapshot")
	rtest.Assert(t, sn2.String() == matchesReverse[1].SnapshotID, "snapshot[1] must match new snapshot")
	rtest.Assert(t, matches[0].SnapshotID == matchesReverse[1].SnapshotID, "matches should be sorted 1")
	rtest.Assert(t, matches[1].SnapshotID == matchesReverse[0].SnapshotID, "matches should be sorted 2")
}

func TestFindInvalidTimeRange(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	err := runFind(context.TODO(), findOptions{Oldest: "2026-01-01", Newest: "2020-01-01"}, env.globalOptions, []string{"quack"}, env.globalOptions.Term)
	rtest.Assert(t, err != nil && err.Error() == "Fatal: --oldest must specify a time before --newest",
		"unexpected error message: %v", err)
}

// JsonOutput is the struct `vaultic find --json` produces
type JSONOutput struct {
	ObjectType string    `json:"object_type"`
	ID         string    `json:"id"`
	Path       string    `json:"path"`
	ParentTree string    `json:"parent_tree,omitempty"`
	SnapshotID string    `json:"snapshot"`
	Time       time.Time `json:"time"`
}

func TestFindPackfile(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	testSetupBackupData(t, env)

	// backup
	backupPath := env.testdata + "/0/0/9"
	testRunBackup(t, "", []string{backupPath}, backupOptions{}, env.globalOptions)
	sn1 := testListSnapshots(t, env.globalOptions, 1)[0]

	// do all the testing wrapped inside withTermStatus()
	err := withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		printer := progress.NewTerminalPrinter(globalOptions.JSON, globalOptions.Verbosity, globalOptions.Term)
		_, repo, unlock, err := openWithReadLock(ctx, globalOptions, false, printer)
		rtest.OK(t, err)
		defer unlock()

		// load master index
		rtest.OK(t, repo.LoadIndex(ctx, printer))

		packID := vaultic.ID{}
		done := false
		err = repo.ListBlobs(ctx, func(pb vaultic.PackBlob) {
			h := pb.Handle()
			if !done && h.Type == vaultic.TreeBlob {
				packID = pb.PackID()
				done = true
			}
		})

		rtest.OK(t, err)
		rtest.Assert(t, !packID.IsNull(), "expected a tree packfile ID")
		findOptions := findOptions{PackID: true}
		results := testRunFind(t, true, findOptions, env.globalOptions, packID.String())

		// get the json records
		jsonResult := []JSONOutput{}
		rtest.OK(t, json.Unmarshal(results, &jsonResult))
		rtest.Assert(t, len(jsonResult) > 0, "expected at least one tree record in the packfile")

		// look at the last record
		lastIndex := len(jsonResult) - 1
		record := jsonResult[lastIndex]
		rtest.Assert(t, record.ObjectType == "tree" && record.SnapshotID == sn1.String(),
			"expected a tree record with known snapshot id, but got type=%s and snapID=%s instead of %s",
			record.ObjectType, record.SnapshotID, sn1.String())
		backupPath = filepath.ToSlash(backupPath)[2:] // take the offending drive mapping away
		rtest.Assert(t, strings.Contains(record.Path, backupPath), "expected %q as part of %q", backupPath, record.Path)

		return nil
	})
	rtest.OK(t, err)
}

func TestFindPackID(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	testSetupBackupData(t, env)

	dir009 := filepath.Join(env.testdata, "0", "0", "9")
	dirEntries, err := os.ReadDir(dir009)
	rtest.OK(t, err)
	numberOfFiles := len(dirEntries)

	// backup
	testRunBackup(t, "", []string{dir009}, backupOptions{}, env.globalOptions)
	sn1 := testListSnapshots(t, env.globalOptions, 1)[0]

	// extract packfile ID from repository index
	dataPackID := vaultic.ID{}
	treePackID := vaultic.ID{}
	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		printer := progress.NewTerminalPrinter(globalOptions.JSON, globalOptions.Verbosity, globalOptions.Term)
		_, repo, unlock, err := openWithReadLock(ctx, globalOptions, false, printer)
		rtest.OK(t, err)
		defer unlock()

		// load Index
		rtest.OK(t, repo.LoadIndex(ctx, vaultic.NoopTerminalCounterFactory))
		// go through all index entries and collect data and tree packfile(s)
		rtest.OK(t, repo.ListBlobs(ctx, func(blob vaultic.PackBlob) {
			switch blob.Handle().Type {
			case vaultic.DataBlob:
				dataPackID = blob.PackID()
			case vaultic.TreeBlob:
				treePackID = blob.PackID()
			}
		}))
		return nil
	})
	rtest.OK(t, err)

	// look for data packfile
	rtest.Assert(t, !dataPackID.IsNull(), "expected to find data packfile in repo")
	packID := dataPackID.String()
	out := testRunFind(t, true, findOptions{PackID: true}, env.globalOptions, packID)

	findRes := []JSONOutput{}
	rtest.OK(t, json.Unmarshal(out, &findRes))
	rtest.Assert(t, len(findRes) == numberOfFiles, "expected %d entries for this packfile, got %d",
		numberOfFiles, len(findRes))

	// corrupt the data pack file; find must fall back to the index
	be := captureBackend(&env.globalOptions)
	err = withTermStatus(t, env.globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		printer := progress.NewTerminalPrinter(globalOptions.JSON, globalOptions.Verbosity, globalOptions.Term)
		_, _, unlock, err := openWithExclusiveLock(ctx, globalOptions, false, printer)
		rtest.OK(t, err)
		defer unlock()

		h := backend.Handle{Type: backend.PackFile, Name: dataPackID.String()}
		rtest.OK(t, be().Remove(ctx, h))
		buf := make([]byte, 10)
		rtest.OK(t, be().Save(ctx, h, backend.NewByteReader(buf, be().Hasher())))
		return nil
	})
	rtest.OK(t, err)

	out = testRunFind(t, true, findOptions{PackID: true}, env.globalOptions, dataPackID.String())
	findRes = []JSONOutput{}
	rtest.OK(t, json.Unmarshal(out, &findRes))
	rtest.Assert(t, len(findRes) == numberOfFiles, "expected %d entries for broken packfile, got %d",
		numberOfFiles, len(findRes))

	// look for tree packfile
	rtest.Assert(t, !treePackID.IsNull(), "expected to find tree packfile in repo")
	packID = treePackID.String()
	out = testRunFind(t, true, findOptions{PackID: true}, env.globalOptions, packID)

	findRes = []JSONOutput{}
	rtest.OK(t, json.Unmarshal(out, &findRes))
	record := findRes[len(findRes)-1]

	rtest.Equals(t, record.ObjectType, "tree")
	rtest.Equals(t, record.SnapshotID, sn1.String())
	// windows path are messy, so we get rid of the messy bits at the start
	// exp: "/C/Users/RUNNER~1/AppData/Local/Temp/vaultic-test-2921201257/testdata/0/0/9"
	// got: "C:/Users/RUNNER~1/AppData/Local/Temp/vaultic-test-2921201257/testdata/0/0/9"
	rtest.Equals(t, filepath.ToSlash(record.Path)[2:], filepath.ToSlash(dir009)[2:])
}
