# Analytics Engine

[← Back to architecture index](00-overview.md)

See also: [Security & Verification Architecture](../03-compliance/08-security-and-verification-architecture.md)
for the Azure Key Vault, syslog, GDPR erasure, and sampled-verification designs
that build on these facts.

## 14. Data growth, churn, per-user/group attribution, and GDPR compliance

### 14.1 Objective

Repository growth and churn are not uniform across time, paths, ownership,
source topology, file size, or source residency. In an enterprise setting with
hundreds of users and 500+ TB / 1.4B inodes, operators need both common rollups
and selective, high-dimensional answers such as:

> How many files, and how many logical bytes, were created by UID 600 during
> 2024, are between 1 MiB and 10 MiB, and remain in retained snapshots although
> they no longer exist on the live source filesystem?

Phase 16 therefore tracks logical file-creation facts by UID, GID, calendar
year/month, ISO week-year/workweek, SVM, volume, optional custom path group,
decimal file-size magnitude, and live-source residency. It supports arbitrary
conjunctions and grouping over these dimensions. Existing growth, churn,
user/group, and GDPR reports become materialized views over the same facts.

"Creation" has an explicit basis. Use filesystem birth time when the source and
protocol expose a trustworthy value; otherwise use the first verified sighting
of a file identity by vaultic, never `mtime`. Persist `creation_basis` as
`birth_time` or `first_seen`, and identify a creation by repository, filesystem,
inode, and observed identity generation so inode reuse cannot merge unrelated
files. Logical bytes are file size at creation. Deduplicated physical bytes are
a separate blob/pack metric and must not be presented as logical creation size.

Analytics is optional and disabled until explicitly enabled. Disable stops all
analytics computation; `disable --purge` removes every derived fact and cache
record. Enabling or rebuilding reconstructs the complete analytics namespace
from authoritative inode revisions and current pointers, so backups made while
analytics was disabled remain queryable after a later rebuild. No backup,
restore, forget, placement, or GC correctness decision depends on analytics.
Rebuild marks analytics disabled before replacing derived records and enables it
only after the complete replacement is durable, so interruption fails closed.
While enabled, successful inode reconciliation triggers a best-effort derived
refresh after the authoritative commit; missed refreshes never fail a backup
and are repaired by the next rebuild.

Trust is a source-capability decision, not a per-process guess. Repository
configuration records which filesystem/protocol identity and birth-time fields
were validated. A supported birth time is accepted only when non-zero and no
later than first observation plus a configurable clock-skew allowance (default
24 hours); otherwise emit an anomaly and use first-seen. The first authoritative
writer persists this immutable choice, so concurrent clients cannot choose
different bases for one generation.

### 14.2 Methodology

#### Invariants and Metadata Sources

POSIX `stat()` calls during backup crawls already supply file UID, GID, size,
and, where available, birth time. Inode records
(`iv:<fsid>:<inode>:<revision-seq>`) persist ownership and identity. Reconciliation
emits a creation fact only for a newly verified identity generation and updates
its source-residency state on later complete crawls.

Source residency is tri-state: `live`, `deleted`, or `unknown`. A file becomes
`deleted` only after a complete authoritative crawl proves its binding absent;
an interrupted crawl, crawl debt, inaccessible subtree, or incomplete
`pathdiff` coverage yields `unknown`. `archive_only` means `deleted` from the
live source and still reachable from at least one retained snapshot. Once no
retained snapshot references the identity it is `expired`, not `archive_only`.
This prevents an outage from being reported as mass source deletion.

A full-crawl deletion proof consists of a successfully committed scan scope,
root identity, start/end fence, and zero unresolved debt for that scope. Absence
proves deletion only inside that scope. A selective `pathdiff` crawl can prove a
deletion only when it starts from a prior complete baseline, verifies a
contiguous event interval through its end fence, and contains an explicit
delete/rename event for the binding. Merely omitting a path from a changed-path
result never proves deletion. Later complete evidence can move `unknown` to
`live` or `deleted`; partial evidence cannot.

Analytics uses logical snapshot membership, not physical GC completion.
`retained_snapshot_refs` changes in the same authoritative transaction that
publishes or removes snapshot membership. Forget makes a fact `expired` when
the last retained logical reference commits; prune and deletion-queue work do
not change that answer. A query reads source state and retained references at
one analytics watermark, so it cannot observe half of an `archive_only`
transition. Analytics is observational and never authorizes pack promotion,
eviction, or GC; those operations retain their own lifecycle and reachability
guards.

#### Source Topology and Custom Path Aggregation

The default topology resolver interprets the first two components relative to
the configured source root as SVM and volume, for example
`/SVM_name/volume_name/...`. Production configurations should normally use an
explicit source-root or LIF-to-SVM/volume mapping so mount aliases cannot split
one volume or merge different volumes. Unresolved topology is stored as
`unknown`; it is never guessed.

Custom aggregation rules are versioned repository configuration:

```yaml
analytics:
  topology:
    - source: /netapp
      layout: svm-volume
  path_groups:
    - svm: svm_finance
      volume: home
      mode: first-subdir       # one group per qtree/first component
    - svm: svm_research
      volume: projects
      mode: depth
      depth: 2
    - svm: svm_shared
      volume: data
      prefixes: [engineering, legal/holds]
```

SVM, volume, and path-group strings are interned in dictionary records and facts
store compact integer IDs. A file belongs to one leaf custom group per rule set;
ancestor rollups are derived at query time to prevent double counting. Changing
rules increments a `classification_epoch` and launches a checkpointed rebuild;
queries expose mixed/unknown classification until that rebuild completes.

Configuration validation rejects two non-prefix modes for the same SVM/volume
scope and duplicate prefixes with different labels. For prefix rules, the
longest normalized component-boundary prefix wins; equal-specificity ambiguity
is an error, not configuration-order behavior. During reclassification, queries
continue to serve the complete old epoch or explicitly request the incomplete
new epoch. An atomic epoch pointer flips only after all facts and indexes for the
new rules reach the same watermark.

#### High-Dimensional Fact Store and Indices

Do not materialize one mutable key for every possible dimension tuple. That
creates a sparse Cartesian cube, hot-key contention, and unbounded rollups as
dimensions are added. Store immutable, append-oriented fact segments plus a
small mutable residency overlay:

```text
key:   af:<segment-id>
value: columnar creation facts
       identity, uid, gid, created_at, creation_basis,
  identity_continuity,
  year, month, iso_week_year, workweek,
       svm_id, volume_id, path_group_id,
  logical_size, size_log10

key:   ar:<fsid>:<inode>:<identity-generation>
value: live_state, last_complete_crawl, retained_snapshot_refs,
       classification_epoch, fact_segment, row

key:   ad:<dictionary-kind>:<id>
value: canonical svm, volume, or path-group string

key:   ai:<dimension>:<value>:<segment-id>
value: compressed row bitmap plus count and logical-byte summary

key:   am:<segment-id>
value: row count, column min/max, bloom filters, creation/revision watermarks
```

`size_log10` is `floor(log_10(max(logical_size_bytes, 1)))`: bucket 0 is zero
through 9 bytes, bucket 1 is 10 through 99 bytes, and so on. More generally
bucket $n$ is $[10^n,10^{n+1})$ bytes; unknown sizes are a separate value.
Store exact `logical_size` as well because a predicate such as 1 MiB through
10 MiB cannot be answered correctly from the magnitude bucket alone.

`--size-min` is inclusive, `--size-max` is exclusive, and unknown sizes never
satisfy a range predicate. SI suffixes use powers of 1000 and IEC suffixes
powers of 1024, so `--size-min 1MiB --size-max 10MiB` means
$1048576 \le size < 10485760$. Workweek always includes its ISO week-year
because week 1 can belong to the previous calendar year. Calendar month is
stored and indexed independently of ISO workweek, keeping calendar reporting
exact around ISO year boundaries.

Identity generation is an internal key, not a normal reporting dimension.
Prefer a source-provided generation/file ID or trustworthy birth time. Without
either, allocate a new generation after a proven `deleted` to `live` transition;
if reuse may have occurred between incomplete crawls, mark identity continuity
`unknown` and exclude it from exact creation counts unless the query opts into
incomplete results. A deleted generation remains independently `archive_only`
when retained while a later generation of the same inode can be `live`.

Encode `identity_continuity` as `proven`, `unknown`, or `source_generation`.
A source generation/birth identity is strongest. Otherwise, reappearance after
a complete crawl committed the prior generation as deleted proves a new
generation; reappearance across any incomplete evidence gap is `unknown`.
Default exact queries exclude unknown continuity, report the excluded count and
bytes, and may include them only with `--include-incomplete`. This makes totals
and their completeness explainable without inventing identity continuity.

Indexed dimensions use segment-local 32-bit row ordinals and mandatory Roaring
bitmaps; each `ai:` record exists only for a value present in that segment.
There is no bitmap per absent value and no global 1.4B-bit allocation. A missing
or structurally invalid optional index falls back to a fact-segment scan and is
reported by `--explain`; it must never produce an empty answer. Range predicates
first prune segments using `am:` min/max metadata,
then intersect bitmaps, inspect exact size/time columns, join `ar:`, and aggregate
count and logical bytes. Selective or cached queries should complete in seconds;
a cold low-selectivity scan across 1.4B rows may take minutes and must report
progress, support cancellation, and optionally continue as a resumable job.

#### Commit and Catch-Up Protocol

Authoritative reconciliation and snapshot transactions do not synchronously
rewrite columnar segments. In the same transaction as each inode/snapshot
change, write an idempotent analytics delta to
`ae:<metadata-commit-seq>:<ordinal>`. This outbox is the atomic correctness
boundary and contains creation, live-state, retained-reference, and
classification inputs. A bounded background builder consumes deltas in commit
order. It writes immutable candidate `af:`/`ai:`/`am:` artifacts under a build
ID and writes changed generation-scoped overlays/views before publication. It
then uses one transaction to publish the child manifest, generation completion
marker, watermark, and active metadata pointer. Queries reach
segments only through the published manifest. A crash before publication leaves
unreferenced candidates for cleanup; a crash after publication sees the whole
commit. Child manifests reference their parent and contain only new immutable
delta segments. Point and prefix lookups resolve changed derived records through
the bounded parent chain; explicit tombstones suppress deleted parent records.
The chain is limited to eight delta generations, after which catch-up performs a
full streaming compaction. Replay observes the durable watermark before doing
work and is idempotent. Outbox records are reclaimed only after the published
watermark covers them.

A full rebuild scans authoritative source records in bytewise key order. Its
versioned checkpoint stores the exact last fully consumed source key, candidate
segment IDs, fact count, and applied commit. Each iteration retains at most
`segment_rows` facts; compact rollups are merged into on-disk candidate records,
while overlays and GDPR mappings stream directly. Dictionary IDs are stable
SHA-256-derived 32-bit IDs and collisions fail closed. Content manifests are
visited one record at a time rather than retained with the fact batch. Legacy
offset checkpoints fail decoding and restart after candidate cleanup. Rebuild
and catch-up results report their maximum buffered records and a conservative
working-set estimate.

Queries read only through the highest fully applied analytics watermark and
report both that value and authoritative metadata head, making lag explicit.
No partially applied commit is visible. `--require-current` waits or fails when
the builder lags; the default returns a snapshot-consistent result at the
reported watermark. This keeps authoritative transactions small while giving
bounded, measurable catch-up behavior rather than silently stale results.
Persisted pending/running query jobs pin their manifest generation and all of
its parents; cleanup skips pinned segments, indexes, views, manifests, and
watermarks until the job completes or is cancelled.

Snapshot publish/forget and complete-crawl source changes produce separate
ordered outbox commits. Until both are applied, a query correctly reports the
older complete state. The atomic manifest/`ar:`/`aw:` publication computes
`live`, `archive_only`, and `expired` from source state plus logical retained
references in that same transaction. Physical GC lag or pack deletion never
changes logical residency; an unexpected missing pack is an integrity finding,
not evidence that an archived file expired.

#### Time-Bucket & Path Churn Rollups

To answer growth and churn questions per week, month, year, or tracked path, `vaulticdb` maintains rollup time-series records:

```text
key:   g:time:<granularity>:<timestamp>:<tier>
value: bytes_added, bytes_deleted, net_change, files_added, files_deleted

key:   g:path:<path_prefix>:<granularity>:<timestamp>
value: bytes_added, bytes_deleted, net_change, files_added, files_deleted
```

Persisted granularities: `week`, `month`, `year`.
`path_prefix` is normalized (e.g. `/home`, `/data/projects`, `/var/log` or paths specified via `--track-path`).
Rollups are updated atomically in the same transaction that commits new inode revisions or reconciles deleted snapshots.

These records are rebuildable materialized views, not the authority for
high-dimensional queries. Add SVM, volume, path-group, creation, and residency
views only when query telemetry shows repeated demand; never eagerly generate
all combinations.

#### Per-User and Per-Group Ownership Aggregates

Maintain real-time current storage state and time-series churn per UID and GID:

```text
key:   u:summary:<uid>
value: active_bytes, active_files, unique_blobs_count, unique_blobs_bytes

key:   g:summary:<gid>
value: active_bytes, active_files, unique_blobs_count, unique_blobs_bytes

key:   u:churn:<uid>:<granularity>:<timestamp>
value: bytes_added, bytes_deleted, files_modified, files_deleted
```

- **Active Bytes:** Total uncompressed size of active files currently owned by `uid` across active snapshots.
- **Unique Blobs Bytes:** Attributed storage footprint accounting for deduplication. Deduplicated blobs can be attributed equally across referencing UIDs or attributed to `first_seen_uid`.
- **User Churn:** Incremental delta recorded per backup run / snapshot purge window.

#### User-to-Data Links for GDPR Auditing

To answer GDPR "Right of Access" and "Right to be Forgotten / Erasure" queries (*"Which data produced by User X is in the vault?"*), SlateDB maintains secondary lookup mappings:

```text
key:   u:inodes:<uid>:<fsid>:<inode>
value: latest_revision_seq, path_sample

key:   u:blobs:<uid>:<blob_hash>
value: ref_count, first_seen_timestamp
```

- **GDPR Inspection:** A query for UID `1042` performs a prefix scan on `u:inodes:1042:` and `u:blobs:1042:`. It instantly yields all active paths, inode revisions, blob hashes, and the cold/hot pack IDs where UID 1042's data resides.
- **GDPR Erasure / Policy Handling:** Shows which snapshots hold UID 1042's data and which packs contain those blobs. If a retention lock prevents immediate cold pack deletion, the compliance report explicitly details when the retention window expires (`min_retention_until`).

#### Dynamic Query Cache and Adaptive Materialization

Canonicalize each query into a typed predicate/grouping AST, including units,
bound inclusivity, classification epoch, repository generation, and the
snapshot/residency watermark. Hash that form for cache identity:

```text
key:   aq:result:<query-hash>:<data-watermark>
value: schema version, exact result, scan statistics, created/expires timestamps

key:   aq:heat:<query-hash>
value: decayed hits, misses, total scan bytes/time, last access, result bytes

key:   aq:view:<view-id>:<bucket-key>
value: adaptively materialized count and logical-byte sums
```

The daemon keeps a bounded memory result cache and optional bounded persistent
cache. Admit entries by frequency and avoided scan cost (TinyLFU-style), evict
by recency/frequency per byte, enforce configurable TTL and byte ceilings, and
never let cache writes contend with backup metadata commits. Old-watermark
results are immutable but stale and are reclaimed asynchronously. Queries are
snapshot-consistent; `--allow-stale` may use an older result and must report its
age and watermark.

The data watermark is exactly `(repository-generation,
classification-epoch, analytics-applied-metadata-commit-seq)`. Any newer
applied commit invalidates a latest-result cache entry, even if it probably did
not affect the predicate; dependency-aware reuse is an optional optimization
that must prove non-intersection from outbox dimension summaries. Within one
query, all pages and groups use the same watermark. There is no implicit
read-your-writes guarantee: callers requiring the metadata head use
`--require-current`; stale reads occur only through explicit `--allow-stale`.

A query first pins one visible `aw:` value, then consults only cache/view entries
with that exact key and scans only its manifest. If `aw:` advances concurrently,
the query may return its pinned, correctly labeled snapshot; a subsequent latest
query pins the new value and cannot hit the old key. Cache insertion cannot make
an old entry current. `--require-current` pins only after `aw:` equals the
authoritative metadata head or fails its wait timeout.

Repeated expensive query shapes may be promoted asynchronously into `aq:view:`
partial cuboids. Admission requires minimum hit count and measured scan cost;
demotion follows a cooling period. Views carry the source watermark and
classification epoch, update incrementally where possible, and fall back to the
authoritative fact scan if incomplete. Cache/view records are disposable.

### 14.3 Processing, Storage, and Feasibility Analysis

#### Storage Overhead

For 1.4B current file identities, dimensional analytics is feasible but not
negligible. Planning estimates after block prefix, column, and bitmap compression:

| component | effective bytes per fact | 1.4B-fact estimate |
|---|---:|---:|
| columnar creation facts and segment metadata | 27-49 | 38-69 GB |
| compressed dimension bitmaps | 9-33 | 13-46 GB |
| mutable source-residency overlay | 14-30 | 20-42 GB |
| topology/path dictionaries and base rollups | 1-8 | 1-11 GB |
| **core incremental total** | **53-123** | **74-172 GB** |

A 10 GB persistent query cache gives an expected deployed range of roughly
**85-185 GB**. Reserve **250 GB** until representative benchmarks measure
UID/GID cardinality, custom path lengths, bitmap density, compression, and LSM
write amplification. The total is approximately 20-46% on top of the roadmap's
existing 350-400 GB metadata estimate, and about 0.02-0.05% of 500 TB.

Creation history grows with newly observed identities, not every unchanged
backup. At 0.1% new identities/day, 1.4M new facts/day add approximately
27-63 GB/year before expired-history compaction; at 1%, 270-630 GB/year requires
an explicit analytics retention/downsampling policy. The `ar:` overlay remains
one current row per identity. Exact GDPR blob mappings remain a separate
estimated ~3.6 GB for 100M `(uid, blob)` relationships.

The estimate assumes 64K-1M-row segments, delta/bit-packed integer columns,
RLE for repeated dimensions, dictionary-coded topology/path values, zstd for
fact blocks, and Roaring bitmaps for `ai:`. SlateDB block compression alone is
not credited for bitmap gains. Benchmarks must publish raw and compressed
bytes/fact, codec settings, index cardinalities, compaction amplification, and
the extrapolation formula; exceeding the 250 GB planning ceiling is a failed
feasibility gate rather than a documentation adjustment.

Use zstd level 3 as the benchmark baseline, RLE runs of at least 100 values, and
Bloom filters sized for at most 1% false positives; persist codec parameters in
`am:` so readers remain compatible with tuning. The 1.4B estimate is not a naive
choice of a favorable sample. Compute fixed database overhead separately, take
the larger observed marginal compressed bytes/fact from 0-10M and 10M-100M,
multiply that upper slope by 1.4B, add projected dictionary/cardinality growth,
10 GB cache, and measured peak compaction space, then report a sensitivity band
of at least +/-20%. Both expected core size (175 GB) and peak deployed space
(250 GB) are independent gates.

Validate at 10M and 100M facts before enabling analytics at 1.4B scale. Phase 16
is feasible if extrapolated core storage is at most 175 GB, reconciliation meets
the limits below, and a representative uncached broad query finishes within the
configured job timeout. Any failed gate blocks general enablement. The feature
remains experimental/off by default until a revised design reduces indexes or
history, or moves analytics to an optional separate SlateDB database so core
metadata is isolated, and the complete benchmark suite passes again.

#### Processing Overhead

1. **Backup/reconciliation:** build segments and bitmaps in bounded batches off
  the scanner hot path from the transactional `ae:` outbox. Target less than
  5% p99 reconciliation CPU overhead, less than 10% authoritative metadata
  write amplification including outbox bytes, and catch-up to metadata head
  within one normal backup interval; verify rather than assume this.
2. **Live-source deletion:** only a complete crawl changes `ar:` to `deleted`.
  Update archive reachability from snapshot commit/forget deltas without
  scanning pack payloads.
3. **Compaction:** merge small segments and bitmaps in the background. Apply
  analytics retention only to expired facts and preserve GDPR-required facts.
4. **Queries:** run below backup, promotion, restore, and forget I/O priority;
  bound concurrent cold scans and persistent-cache construction.

Measure reconciliation CPU as analytics outbox-emission CPU divided by baseline
reconciliation CPU for the same deterministic crawl at p99. Measure authoritative
write overhead as `ae:` bytes divided by baseline inode/directory/snapshot bytes;
background `af:`/`ai:`/`ar:` bytes count toward storage and total-write metrics,
not synchronous reconciliation amplification. Measure catch-up as wall time and
commit distance from metadata head to `aw:` after a standard benchmark backup.
The benchmark profile defines the interval/SLO (default 24 hours), hardware,
cache state, fact distribution, and concurrency; export all raw counters through
metrics and `index analytics status --json`.

#### Reference Feasibility Profile

Publish the hardware, SlateDB settings, and synthetic-data generator seed. The
generator must match measured production distributions for UID/GID cardinality,
SVM/volume/path groups, file sizes, creation dates, source deletion, and retained
snapshot references. Run identical baseline and analytics-enabled crawls at 10M
and 100M facts after warm-up, with at least five measured repetitions:

1. Compare 1,000 randomized filtered/grouped results, including the documented
  UID 600 query, byte-for-byte against a brute-force fact scan at the same
  watermark. Any mismatch fails the phase.
2. Run the cold broad query `--year 2024 --size-min 1MiB --size-max 100MiB
  --group-by uid --group-by svm --group-by residency`. It must finish within
  120 seconds at 100M facts and the conservative 1.4B projection must remain
  within the default 30-minute synchronous timeout.
3. Repeat promoted popular queries from memory and persistent cache. p95 latency
  must be at most two seconds, every result must identify the exact source
  watermark, and advancing `aw:` must prevent a latest query from reusing it.
4. After the standard incremental backup (0.1% new identities and 0.5% residency
  changes), analytics must catch up within the smaller of 24 hours and the
  configured backup interval, with no partially visible commit.
5. p99 outbox-emission CPU overhead must be at most 5%; authoritative write
  overhead from `ae:` must be at most 10%; expected 1.4B core storage must be at
  most 175 GB; and projected peak deployed space must be at most 250 GB.

These are reference general-enablement gates, not universal latency promises.
Deployments expose estimates from their own cardinalities and may set stricter
timeouts or cache budgets. A failed reference gate keeps Phase 16 experimental
and off by default.

### 14.4 CLI Command Surface

`vaultic index growth` — Query growth and churn over time and subdirectories.

- `--granularity week|month|year`
- `--path <prefix>` filter or group by major subdirectories
- `--since`, `--until`
- `--json`

`vaultic index analytics` — Query the high-dimensional creation fact store.

- predicates: `--uid`, `--gid`, `--year`, `--month`, `--iso-year`,
  `--workweek`, `--svm`, `--volume`, `--path-group`, `--size-min`, `--size-max`,
  `--size-log10`, `--creation-basis`, and
  `--residency live|archive-only|unknown|expired`
- repeated values mean set membership; different predicates combine with AND
- repeat `--group-by` for any tracked dimensions
- `--metric files|logical-bytes|both`; exact bounds accept SI and IEC units
- `--explain` reports pruning, indexes, estimated rows, cache/view choice,
  watermark, and classification completeness
- `--async`, `--query-id`, `--wait`, and `--cancel` manage expensive resumable
  queries; `--timeout` defaults to 30 minutes for synchronous and one hour for
  async execution; `--allow-stale <duration>` opts into labeled stale results
- async progress is durably checkpointed under `aq:job:<query-id>` by segment;
  daemon restart leaves it resumable with `--query-id <id> --resume`, while
  cancellation is terminal unless `--restart` creates a new job
- `--require-current` waits for the applied watermark up to `--wait-timeout`
- `--include-incomplete` includes unknown identity continuity and reports it
- `--json` emits canonical query, result, units, creation semantics,
  completeness, cache status, scan statistics, and `query_time`,
  `repository_generation`, `classification_epoch`, `applied_commit_seq`,
  `metadata_head_commit_seq`, and `lag_seconds`

```text
vaultic index analytics --uid 600 --year 2024 \
  --size-min 1MiB --size-max 10MiB --residency archive-only \
  --metric both --explain --json
```

`vaultic index user-stats` — Query user/group storage usage and churn rankings.

- `--top-storage` rank users/groups by total active stored bytes
- `--top-churn` rank users/groups by churn (`bytes_added + bytes_deleted`) over a time window
- `--since`, `--until` (e.g., `--since 2m` for the last 2 months)
- `--group-by user|group`
- `--limit N` (e.g., `--limit 10`)
- `--json`

`vaultic index gdpr audit` — **Phase 16:** GDPR compliance inspection tool.

- `--uid <uid>` or `--username <name>`
- `--gid <gid>`
- `--detail` include exact file paths, inode revisions, blob hashes, and pack IDs
- `--explain-surviving-chunks` show per-chunk breakdown explaining which non-scoped files reference each chunk
- `--json`

Outputs a complete audit report listing active paths, referenced blob IDs, target storage packs (hot vs cold), retention expiry dates (`min_retention_until`), and an analysis of chunks that would survive deletion due to external deduplication references.

`vaultic index gdpr execute-forget` — **Phase 17:** Execute GDPR user data erasure.

- `--uid <uid>`
- `--confirm`
- `--json`

Purges user file references and inode revisions across active snapshots, re-evaluates chunk reference counts, enqueues unreferenced chunks/packs into the deletion queue (`dq:`), and outputs a cryptographic erasure certificate.

`vaultic index gdpr set-policy` — **Phase 17:** Configure backup exclusion policies.

- `--exclude-uid <uid>`
- `--remove-exclusion <uid>`
- `--json`

Configures persistent UID blocklist rules (`u:policy:blocklist:<uid>`) preventing the archiver and reconciliation engine from backing up files owned by erased users in future crawls.

