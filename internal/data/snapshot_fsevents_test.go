package data

import (
	"encoding/json"
	"testing"
	"time"
)

func TestSnapshotFSEventsMetadataJSONRoundTrip(t *testing.T) {
	captured := time.Date(2026, 9, 8, 9, 40, 12, 0, time.UTC)
	original := Snapshot{
		FSEventsAnchors: []FSEventsAnchor{{
			Device: 17, VolumeUUID: "volume", JournalUUID: "journal", EventID: 42,
			CapturedAt: captured, SourceRoots: []string{"/Users/oli"}, APFSSnapshot: "snapshot",
		}},
		CrawlPlan: &CrawlPlan{
			Mode: "selective", Source: "fsevents", ChangedPaths: []string{"/Users/oli/Documents"},
			ReusedSubtrees: 3, APFSSnapshot: "snapshot",
			Roots: []CrawlRootPlan{{Root: "/Users/oli", Mode: "selective", EventCount: 2}},
		},
	}
	payload, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Snapshot
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.FSEventsAnchors) != 1 || decoded.FSEventsAnchors[0].EventID != 42 ||
		decoded.CrawlPlan == nil || decoded.CrawlPlan.Mode != "selective" || decoded.CrawlPlan.ReusedSubtrees != 3 || len(decoded.CrawlPlan.Roots) != 1 {
		t.Fatalf("decoded metadata = %#v", decoded)
	}
}

func TestSnapshotFSEventsMetadataIsOptional(t *testing.T) {
	payload, err := json.Marshal(Snapshot{})
	if err != nil {
		t.Fatal(err)
	}
	if string(payload) == "" || containsJSONField(payload, "fsevents_anchors") || containsJSONField(payload, "crawl_plan") {
		t.Fatalf("empty snapshot unexpectedly contains phase 23 metadata: %s", payload)
	}
}

func containsJSONField(payload []byte, field string) bool {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(payload, &object); err != nil {
		return false
	}
	_, ok := object[field]
	return ok
}
