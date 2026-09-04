package data_test

import (
	"context"
	"testing"
	"time"

	"github.com/otuschhoff/vaultic/internal/data"
	"github.com/otuschhoff/vaultic/internal/repository"
	"github.com/otuschhoff/vaultic/internal/test"
	"github.com/otuschhoff/vaultic/internal/vaultic"
)

func saveFilterSnapshot(t *testing.T, repo vaultic.Repository, at time.Time, label string, tags, paths []string, total, added uint64) *data.Snapshot {
	snapshot, err := data.NewSnapshot(paths, tags, "filter-host", at)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.Label = label
	snapshot.Summary = &data.SnapshotSummary{TotalBytesProcessed: total, DataAdded: added}
	id, err := data.SaveSnapshot(context.Background(), repo, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	snapshot2, err := data.LoadSnapshot(context.Background(), repo, id)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot2
}

func TestSnapshotFilterRusticOptions(t *testing.T) {
	repo := repository.TestRepository(t)
	first := saveFilterSnapshot(t, repo, parseTimeUTC("2020-01-01 00:00:00"), "daily", []string{"one", "two"}, []string{"/a", "/b"}, 100, 10)
	saveFilterSnapshot(t, repo, parseTimeUTC("2020-01-02 00:00:00"), "weekly", []string{"three"}, []string{"/c"}, 200, 20)

	filters := []struct {
		name   string
		filter data.SnapshotFilter
		want   *data.Snapshot
	}{
		{
			"aliases and ranges",
			data.SnapshotFilter{
				FilterHosts:  []string{"filter-host"},
				FilterPaths:  [][]string{{"/a", "/b"}},
				FilterTags:   data.TagLists{{"two"}},
				Labels:       []string{"daily"},
				SizeMin:      90,
				SizeMax:      110,
				SizeAddedMin: 9,
				SizeAddedMax: 11,
			},
			first,
		},
		{"jq", data.SnapshotFilter{FilterJQ: `.label == "daily" and .summary.data_added == 10`}, first},
	}
	for _, test := range filters {
		t.Run(test.name, func(t *testing.T) {
			got, _, err := test.filter.FindLatest(context.Background(), repo, repo, "latest")
			if err != nil {
				t.Fatal(err)
			}
			if *got.ID() != *test.want.ID() {
				t.Fatalf("matched snapshot %v, want %v", got.ID(), test.want.ID())
			}
		})
	}
}

func TestFindLatestSnapshot(t *testing.T) {
	repo := repository.TestRepository(t)
	data.TestCreateSnapshot(t, repo, parseTimeUTC("2015-05-05 05:05:05"), 1)
	data.TestCreateSnapshot(t, repo, parseTimeUTC("2017-07-07 07:07:07"), 1)
	latestSnapshot := data.TestCreateSnapshot(t, repo, parseTimeUTC("2019-09-09 09:09:09"), 1)

	f := data.SnapshotFilter{Hosts: []string{"foo"}}
	sn, _, err := f.FindLatest(context.TODO(), repo, repo, "latest")
	if err != nil {
		t.Fatalf("FindLatest returned error: %v", err)
	}

	if *sn.ID() != *latestSnapshot.ID() {
		t.Errorf("FindLatest returned wrong snapshot ID: %v", *sn.ID())
	}
}

func TestFindLatestSnapshotWithMaxTimestamp(t *testing.T) {
	repo := repository.TestRepository(t)
	data.TestCreateSnapshot(t, repo, parseTimeUTC("2015-05-05 05:05:05"), 1)
	desiredSnapshot := data.TestCreateSnapshot(t, repo, parseTimeUTC("2017-07-07 07:07:07"), 1)
	data.TestCreateSnapshot(t, repo, parseTimeUTC("2019-09-09 09:09:09"), 1)

	sn, _, err := (&data.SnapshotFilter{
		Hosts:          []string{"foo"},
		TimestampLimit: parseTimeUTC("2018-08-08 08:08:08"),
	}).FindLatest(context.TODO(), repo, repo, "latest")
	if err != nil {
		t.Fatalf("FindLatest returned error: %v", err)
	}

	if *sn.ID() != *desiredSnapshot.ID() {
		t.Errorf("FindLatest returned wrong snapshot ID: %v", *sn.ID())
	}
}

func TestFindLatestWithSubpath(t *testing.T) {
	repo := repository.TestRepository(t)
	data.TestCreateSnapshot(t, repo, parseTimeUTC("2015-05-05 05:05:05"), 1)
	desiredSnapshot := data.TestCreateSnapshot(t, repo, parseTimeUTC("2017-07-07 07:07:07"), 1)

	for _, exp := range []struct {
		query     string
		subfolder string
	}{
		{"latest", ""},
		{"latest:subfolder", "subfolder"},
		{desiredSnapshot.ID().Str(), ""},
		{desiredSnapshot.ID().Str() + ":subfolder", "subfolder"},
		{desiredSnapshot.ID().String(), ""},
		{desiredSnapshot.ID().String() + ":subfolder", "subfolder"},
	} {
		t.Run("", func(t *testing.T) {
			sn, subfolder, err := (&data.SnapshotFilter{}).FindLatest(context.TODO(), repo, repo, exp.query)
			if err != nil {
				t.Fatalf("FindLatest returned error: %v", err)
			}

			test.Assert(t, *sn.ID() == *desiredSnapshot.ID(), "FindLatest returned wrong snapshot ID: %v", *sn.ID())
			test.Assert(t, subfolder == exp.subfolder, "FindLatest returned wrong path in snapshot: %v", subfolder)
		})
	}
}

func TestFindAllSubpathError(t *testing.T) {
	repo := repository.TestRepository(t)
	desiredSnapshot := data.TestCreateSnapshot(t, repo, parseTimeUTC("2017-07-07 07:07:07"), 1)

	count := 0
	test.OK(t, (&data.SnapshotFilter{}).FindAll(context.TODO(), repo, repo,
		[]string{"latest:subfolder", desiredSnapshot.ID().Str() + ":subfolder"},
		func(id string, sn *data.Snapshot, err error) error {
			if err == data.ErrInvalidSnapshotSyntax {
				count++
				return nil
			}
			return err
		}))
	test.Assert(t, count == 2, "unexpected number of subfolder errors: %v, wanted %v", count, 2)
}
