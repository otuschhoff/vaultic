package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/global"
	rtest "github.com/otuschhoff/vaultic/internal/test"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func testRunLsWithOpts(t testing.TB, globalOptions global.Options, options lsOptions, args []string) []byte {
	buf, err := withCaptureStdout(t, globalOptions, func(ctx context.Context, globalOptions global.Options) error {
		globalOptions.Quiet = true
		return runLs(context.TODO(), options, globalOptions, args, globalOptions.Term)
	})
	rtest.OK(t, err)
	return buf.Bytes()
}

func testRunLs(t testing.TB, globalOptions global.Options, snapshotID string) []string {
	out := testRunLsWithOpts(t, globalOptions, lsOptions{}, []string{snapshotID})
	return strings.Split(string(out), "\n")
}

func assertIsValidJSON(t *testing.T, data []byte) {
	// Sanity check: output must be valid JSON.
	var v []any
	err := json.Unmarshal(data, &v)
	rtest.OK(t, err)
	rtest.Assert(t, len(v) == 4, "invalid ncdu output, expected 4 array elements, got %v", len(v))
}

func TestRunLsNcdu(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	testSetupBackupData(t, env)
	options := backupOptions{}
	// backup such that there are multiple toplevel elements
	testRunBackup(t, env.testdata+"/0", []string{"."}, options, env.globalOptions)

	for _, paths := range [][]string{
		{"latest"},
		{"latest", "/0"},
		{"latest", "/0", "/0/9"},
	} {
		ncdu := testRunLsWithOpts(t, env.globalOptions, lsOptions{Ncdu: true}, paths)
		assertIsValidJSON(t, ncdu)
	}
}

func TestRunLsSort(t *testing.T) {
	rtest.Equals(t, SortMode(0), SortModeName, "unexpected default sort mode")

	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	testSetupBackupData(t, env)
	options := backupOptions{}
	testRunBackup(t, env.testdata+"/0", []string{"for_cmd_ls"}, options, env.globalOptions)

	for _, test := range []struct {
		mode     SortMode
		expected []string
	}{
		{
			SortModeSize,
			[]string{
				"/for_cmd_ls",
				"/for_cmd_ls/file2.txt",
				"/for_cmd_ls/file1.txt",
				"/for_cmd_ls/python.py",
				"",
			},
		},
		{
			SortModeExt,
			[]string{
				"/for_cmd_ls",
				"/for_cmd_ls/python.py",
				"/for_cmd_ls/file1.txt",
				"/for_cmd_ls/file2.txt",
				"",
			},
		},
		{
			SortModeName,
			[]string{
				"/for_cmd_ls",
				"/for_cmd_ls/file1.txt",
				"/for_cmd_ls/file2.txt",
				"/for_cmd_ls/python.py",
				"", // last empty line
			},
		},
	} {
		out := testRunLsWithOpts(t, env.globalOptions, lsOptions{Sort: test.mode}, []string{"latest"})
		fileList := strings.Split(string(out), "\n")
		rtest.Equals(t, test.expected, fileList, fmt.Sprintf("mismatch for mode %v", test.mode))
	}
}

// JSON lines test
func TestRunLsJson(t *testing.T) {
	pathList := []string{
		"/0",
		"/0/for_cmd_ls",
		"/0/for_cmd_ls/file1.txt",
		"/0/for_cmd_ls/file2.txt",
		"/0/for_cmd_ls/python.py",
	}

	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	testSetupBackupData(t, env)
	options := backupOptions{}
	testRunBackup(t, env.testdata, []string{"0/for_cmd_ls"}, options, env.globalOptions)
	snapshotIDs := testListSnapshots(t, env.globalOptions, 1)

	env.globalOptions.Quiet = true
	env.globalOptions.JSON = true
	buf := testRunLsWithOpts(t, env.globalOptions, lsOptions{}, []string{"latest"})
	byteLines := bytes.Split(buf, []byte{'\n'})

	// partial copy of snapshot structure from cmd_ls
	type lsSnapshot struct {
		*data.Snapshot
		ID          *vaultic.ID `json:"id"`
		ShortID     string      `json:"short_id"`     // deprecated
		MessageType string      `json:"message_type"` // "snapshot"
		StructType  string      `json:"struct_type"`  // "snapshot", deprecated
	}

	var snappy lsSnapshot
	rtest.OK(t, json.Unmarshal(byteLines[0], &snappy))
	rtest.Equals(t, snappy.ShortID, snapshotIDs[0].Str(), "expected snap IDs to be identical")

	// partial copy of node structure from cmd_ls
	type lsNode struct {
		Name        string `json:"name"`
		Type        string `json:"type"`
		Path        string `json:"path"`
		Permissions string `json:"permissions,omitempty"`
		Inode       uint64 `json:"inode,omitempty"`
		MessageType string `json:"message_type"` // "node"
		StructType  string `json:"struct_type"`  // "node", deprecated
	}

	var testNode lsNode
	for i, nodeLine := range byteLines[1:] {
		if len(nodeLine) == 0 {
			break
		}

		rtest.OK(t, json.Unmarshal(nodeLine, &testNode))
		rtest.Equals(t, pathList[i], testNode.Path)
	}
}
