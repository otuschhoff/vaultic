# Phase 10: Pack history event log and rollups

[← Back to roadmap index](00-overview.md)

[← Phase 9](phase-09-pack-tier-model-and-lifetime-facts.md) · [Phase 11 →](phase-11-introspection-cli.md)

**Goal:** durable, append-only history of pack lifecycle transitions that
survives deletion of the packs it describes.

**Current implementation state (2026-08-29):** **complete.** The `ph:`
namespace records an event in the same transaction as every pack catalog
transition — created, imported, published, tier changed, coalesced usage
change, repack lineage, delete-pending, deleted — plus the observations that
are not transitions: `repacked_from` on a superseded source, `delete_failed`
when a physical removal fails, and `orphan_detected` when `check` finds a pack
on the backend that no index references.

Events are ordered by time, then by a globally monotonic sequence allocated
inside the caller's transaction. The sequence is what makes keys unique when
two writers record events for the same pack within one second, and what gives
the log a total order independent of clock resolution; a concurrent-writer test
asserts both properties.

Each event is self-contained, carrying the pack's classification and sizes
rather than referring back to `p:`. That is what lets history stay readable for
a pack whose catalog record was removed in the same transaction that recorded
its deletion.

Repack destinations carry `predecessor_pack_ids`, declared through a repack
context set around the copy because the destination pack IDs are chosen inside
it. Lineage is therefore reconstructable across several generations, and a
rewrite is never counted as growth. Usage events are coalesced to one per pack
per update regardless of how many blobs changed reachability.

The `pb:` namespace holds hourly, daily, and monthly rollups computed as a pure
function of the retained raw events, so recomputation is idempotent and equal to
a direct scan. Retention rolls up before truncating and records a raw floor that
only ever advances, so a range whose raw events were intentionally discarded is
reported `partial` rather than silently `complete`. Buckets covering a period
before collection was enabled, and buckets containing legacy imports — which
describe packs that existed before the import ran — are reported
`reconstructed`.

History is advisory throughout. Raw events are append-only: they may be written
only by the transition that produced them, never rewritten through a generic
batch, though retention may prune them. A corrupt or missing record is counted
and skipped by every reader, and `index check` reports the count without
changing its verdict. A fault-injection test corrupts every kind of history
record at once and asserts that export, check, and aggregate rebuild all still
succeed and reach the same verdict.

**Deviations from the plan:** the record carries a backend identifier that stays
zero until Phase 12 supplies one. It is present from the start because the log
is append-only and immutable, so adding the field later would mean either
rewriting history or reading two layouts forever. `tier_changed` was restored to
the event vocabulary that the [storage placement policy](../02-architecture/05-storage-placement.md) revision had dropped: the Phase 9 tier
is a real, observable transition until placement supersedes it.

**Fixed while implementing:** the maintenance test harness silently discarded
the deletes passed to `WriteMutableBatch`, so no test in that package could ever
have caught a deletion bug. It now applies them, which is what surfaced the
retention behaviour these tests assert.

**Implementation steps:**

1. Add the `ph:` event namespace and write an event in the same transaction as
  every pack catalog transition: create, import, publish, tier change,
  coalesced usage change, repack lineage, delete-pending, delete, delete
  failure, orphan detection.
2. Record `predecessor_pack_ids` on repack events so churn is distinguishable
  from growth.
3. Coalesce `usage_changed` events per pack per run; never emit one per blob.
4. Add the `pb:` rollup namespace with hourly, daily, and monthly
  granularities, computed idempotently from retained raw events, each bucket
  carrying a `complete`/`partial`/`reconstructed` coverage flag.
5. Implement history retention: roll up, then truncate raw events, writing a
  coverage marker for the truncated range.
6. Guarantee history is advisory: a missing, truncated, or corrupt history
  record must never fail or alter backup, restore, prune, or GC. Add a
  fault-injection test that corrupts history and asserts all data paths still
  succeed.
7. Mark buckets covering periods before history collection was enabled, or
  produced by legacy import, as `reconstructed`.

**Tests:** event ordering and key uniqueness under concurrent writers; rollup
idempotence and equality against a direct scan of raw events; coverage flags
after enabling history on an existing repository and after a retention run;
events for deleted packs still readable; repack lineage reconstructable across
several generations; corrupted-history fault injection leaving every data path
green.

**Verification performed:** eight concurrent writers produce unique,
monotonically ordered sequences; a rollup over an unchanged raw range writes
nothing and equals an independent fold of the log; coverage is asserted for all
three cases — before collection, before the retained raw floor, and fully
observed — plus the import case; a pack deleted from both the backend and the
catalog still has its four events readable; lineage is followed back through
three generations; corrupting a raw event, a bucket, and both markers at once
leaves export, check, and aggregate rebuild green and the verdict unchanged; a
usage update across six blobs emits exactly one event; retention is proven to
roll up before truncating, to leave the truncated range's totals intact in its
bucket, to never lower the raw floor, and to prune each granularity on its own
schedule. The exit criterion runs end to end against a real repository that was
backed up, imported, repacked, and pruned: its history contains origin,
publication, usage, delete-pending, deletion, and both repack lineage
directions; it still describes packs that no longer exist; and its rollup is
idempotent and correctly reports imported activity as reconstructed rather than
observed. Full suites pass under `-race`, and the `CGO_ENABLED=0` build is
retained.

**Exit criterion:** a repository that has been backed up, repacked, and pruned
can report its full pack history, including for packs no longer present, with
correct coverage flags.
