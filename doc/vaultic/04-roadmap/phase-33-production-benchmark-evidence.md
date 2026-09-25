# Phase 33 Production Benchmark Evidence

## Recursive component filter reduction, r7/r8 (2026-09-25)

The r6 profile exposed recursive wildcard expansion in exclusions. Patterns
`**/component` and `**/component/**` now reduce to the existing component-search
matcher, retaining original pattern text, negation, malformed-component errors,
and the old minimum path length for the trailing recursive form. The first
literal-only candidate caught a one-component relative-path edge case in tests;
the final candidate preserves it and supports glob components as well.
An explicit wildcard-expansion oracle covers literals, wildcards, classes,
escapes, malformed patterns, roots and short paths. The full filter race suite
and affected crawl/backup tests pass.

R7 (literals only) still spent 190.45 sampled CPU seconds in filter.match,
versus r6's 251.52; the remaining recursive Trash wildcard was active in stacks.
R8 includes glob components. Both runs use the same cwalk32/52-root/HDD-NFS
scope and ten-minute cap, reach manifest preparation after catalog reads plateau
around 205 seconds, and cancel cleanly without publishing a snapshot.

| Metric | r7 | r8 |
| --- | ---: | ---: |
| Wall seconds | 603.423 | 603.015 |
| CLI CPU seconds | 1,220.20 | 1,072.40 |
| Peak CLI RSS, KiB | 30,445,032 | 29,752,232 |
| Daemon CPU seconds | 734.47 | 733.53 |
| Main GETs | 42,044 | 42,064 |
| Logical body bytes | 43,074,542,380 | 43,107,706,747 |

These are bounded runs, not equal completed-work measurements: manifest record
progress is not yet exposed, so lower CPU is not a proven throughput ratio.
All 143 snapshot IDs remain unchanged; metadata writes/commits and sampler errors
are zero. R8's late stack moves to metadata exclusion checks, including marker
file Lstat inside serialized selection. Evidence is retained under
`db.test/backup-cwalk-filter-2026-09-25-r7` and
`db.test/backup-cwalk-component-2026-09-25-r8`.

## Cwalk metadata read concurrency, r6 (2026-09-25)

Manifest selection previously held one mutex across name selection, a second
filesystem Lstat, and metadata selection. The lock now protects only selection
callbacks; cwalk's bounded workers can overlap the filesystem reads. A gated
regression proves two reads overlap while all selection callbacks remain
serialized. Cwalk parity and cancellation race checks pass against upstream
v1.0.1, with the known root-permission fallback test excluded.

R6 retains 52 roots, 32 cwalk workers, HDD-NFS scratch and the ten-minute cap.
It reaches the manifest phase after catalog reads plateau around 205 seconds,
then exits on TERM at 603.151 seconds without a forced kill. All 143 snapshot
IDs remain unchanged; engine writes/commits are zero. CLI CPU is 1,164.74 seconds
and peak RSS 33,063,352 KiB. The 603.956-second daemon window records 743.11 CPU
seconds, 41,981 GETs, 66.690693 aggregate GET-service seconds and
42,909,993,282 logical body bytes. No sampler errors occurred.

Catalog startup varies substantially from r5, before the changed code executes,
so this does not establish a production runtime win. The manifest still does
not complete. Its next measured CPU cost is exclusion matching: filter.match
accounts cumulatively for 251.52 of 1,159.53 sampled CPU seconds; later stacks
also show selection-lock contention. Optimize matching semantics-preservingly
before considering callback concurrency. Evidence is retained under
`db.test/backup-cwalk-metadata-2026-09-25-r6`.

## Cwalk cancellation and archiver handoff, r4/r5 (2026-09-24)

After catalog parallelism reached source traversal, cancellation required fixes
at two boundaries. Cwalk's worker loops and startup waits did not observe Stop;
that patch and its deterministic regression are now upstream in cwalk v1.0.1
(a5cd220), adopted by Vaultic in aa4bf5c7b. The Vaultic wrapper checks cancellation
before each root and after walker registration, stops callbacks on cancellation,
and joins its cancellation monitor. The archiver also checks cancellation before
snapshot preparation and after manifest preparation, rather than initializing
upload work after a canceled manifest has been discarded.

R4 still required the kill grace despite the cwalk worker fixes. R5 adds the
archiver handoff checks and exits at 602.951 seconds with timeout status 124,
about three seconds after TERM, instead of requiring SIGKILL at 645 seconds.
Its final error is context cancellation, and no 610-second post-TERM stack is
present because the process had already exited. Both runs explicitly use cwalk
with 32 workers, the original 52 roots, and HDD-NFS manifest scratch; r3 had
inherited the default temporary directory and is not a controlled scratch
performance comparison. In-flight filesystem calls remain non-interruptible.

R5 records 766.44 user and 93.46 system CPU seconds, peak RSS 32,555,540 KiB,
and no completed backup. Artifacts, source patch and the then-local cwalk source
are retained under `db.test/backup-cwalk-cancel-2026-09-24-r4` and `-r5`.
Cancellation/temporary-state cleanup, traversal parity, and the regression that
forbids uploader startup after cancellation pass under the race detector against
upstream v1.0.1. The known root-only permission fallback test remains excluded.
This is a shutdown improvement, not a completed-backup throughput result.

## Four catalog workers reach cwalk, r3 (2026-09-24)

The third candidate uses four private compact builders, each scanning disjoint
blob-key partitions under the same pinned read session. Worker failure cancels
and joins the group; all projections remain private until completion and final
validation. Per-chunk counters report processed records, locations and completed
partitions on interrupted startup. Native multi-page lookup/cancellation tests
and affected-package race tests passed, including concurrent counter updates.

Production r3 used the same explicit cwalk, 52-root scope, ten-minute cap and
unchanged primary. Catalog reads plateaued near 205s; the 180s stack is still
in catalog loading, while both 360s and 550s stacks prove the command reached
`Archiver.prepareCWalkManifest -> BuildDirectoryManifest -> cwalk.Walker.Run`.
Thus startup now reaches source traversal within the feedback window, unlike
both single-consumer candidates and the original hour-long unary attempt.
This is not a completed-backup runtime ratio: no snapshot was published.

The newly reached cwalk manifest pass failed to respond to TERM within the
45-second grace. Timeout killed the process group at 645.007s (exit 137).
GNU time then reported wrapper-only CPU/RSS and the CPU profile was not cleanly
finalized; those totals must not be treated as backup resources. The saved
analyzer instead reports the 640.621s sampled CLI window: 1,044.78 CPU seconds
and 30,146,520 KiB sampled peak RSS. Daemon status spans 645.738s with 733.20
CPU seconds, 42,226 GETs, 96.971324 aggregate service seconds and
43,176,477,462 logical body bytes. All 143 snapshot IDs remain unchanged,
engine writes/commits are zero, and the original primary remains healthy at
epoch 55 with no active transactions/intents or remaining backup process.

The cwalk stacks show directory reads in the external walker and the caller
waiting in `Walker.Run`; authoritative reconciliation workers are still idle.
The next required resilience work is cancellation and bounded manifest
preparation while retaining cwalk. Artifacts and corrected sampled-resource
analysis are under `db.test/backup-parallel4-2026-09-24-r3`.

## Direct compact backup projection, r2 (2026-09-24)

The second candidate removes the full `map[PackID][]Blob` staging copy. A
single-owner catalog builder writes directly into the existing compact index,
reuses one ordinal per pack, validates types and 32-bit offset/length bounds,
and releases its pack map when finished. Normal and pending-export projections
remain separate and private until the entire read session validates and closes;
cancellation still installs neither. The native multi-page parity/cancellation
test passed, as did full compact-index, index-engine and backup-package race
tests. Builder tests cover ordinal reuse, duplicate locations, invalid-input
non-mutation and finish semantics.

A 65,536-location synthetic benchmark, three repetitions, measured the old
map-then-copy path at 14.48-14.82ms and 17.47MB/2,082 allocations, versus direct
construction at 9.40-9.56ms and 9.07MB/673 allocations. This is a local allocation
and construction benchmark, not a backup runtime measurement.

Production r2 used the same ten-minute cwalk-enabled 52-root scope, unchanged
primary and settings, from `22d7eab89` plus its recorded patch. It timed out at
602.401s in catalog loading, with unchanged 143 snapshot IDs and zero writes or
commits. CLI CPU was 570.81s and peak RSS 26,634,612 KiB, versus r1's 573.64s
and 38,989,088 KiB. R2 consumed 36,147,888,989 logical body bytes versus r1's
38,922,261,970: memory is lower, but less catalog data was reached, so there is
no demonstrated production runtime win or exact equal-scope memory percentage.
The 603.174s daemon window recorded 562.19 CPU seconds, 36,060 GETs and
80.576054 aggregate GET-service seconds. No sampler errors or leaked sessions
occurred; the original primary stayed healthy at epoch 55.

The new CPU profile shows compact `indexMap.add` at 30.79% cumulative and its
preallocation at 11.89%, with map lookup and protobuf/schema decoding also
material. One scan/build consumer remains CPU-active at about one core.
The next bounded candidate is independent parallel scan/build workers sharing
the same pinned snapshot but not their mutable projections. Artifacts are in
`db.test/backup-compact-2026-09-24-r2`.

## Backup catalog streaming, r1 (2026-09-24)

The first backup optimization replaces unary pending-pack/blob catalog pages
with the existing renewed read-session streaming API. Both catalogs use one
pinned snapshot, final generation validation precedes projection installation,
and bounded cancellation-independent rollback closes the session before backup
writes. Streaming validation and legacy-daemon fallback remain in the shared
API. A native 12,002-location test covers mixed types, shared blob locations,
pending-export projection, cancellation without partial installation, retry,
and zero remaining transactions/intents. Focused index, read-session, repository
and backup/cwalk race tests passed; final native regression passed in 2.961s.

A fresh profile CLI from `4ab4f5a93` plus the recorded patch ran the unchanged
52-root scope with explicit `--use-cwalk --cwalk-concurrency=32`. The diagnostic
retained the ten-minute cap, 45-second grace, HDD-NFS primary and unchanged
daemon; no builds/tests overlapped. It timed out after 603.101s, still loading
the catalog. All four captured stacks remained in that phase. Both inventories
retain the same 143 snapshot IDs, with zero engine writes/commits, and the
primary remained PID 431717, epoch 55, healthy with no transactions/intents.

CLI CPU was 497.90 + 75.74s, peak RSS 38,989,088 KiB, and swaps zero.
The 603.918s daemon window recorded 605.14 CPU seconds, 38,706 main-store GETs,
101.002503 aggregate GET-service seconds and 38,922,261,970 logical body bytes.
The prior ten-minute unary attempt recorded 504,245 GETs and 531.009527 service
seconds, but neither completed and record progress was unavailable: these
figures demonstrate changed request behavior, not an end-to-end speedup ratio.
The streaming run is now approximately one CLI core busy; its CPU profile
attributes 64.44% cumulatively to the catalog consumer, including map growth,
copying and decoding. The intermediate per-pack blob map reached about 37 GiB
RSS before it could be copied into the compact lookup projection.

The next bounded candidate is direct construction of a private compact
projection, avoiding the full duplicate intermediate representation while
retaining all-or-nothing installation. Parallel scan consumption and explicit
record progress remain separate opportunities. Artifacts, source patch,
profiles, samples, analyzer, identities and checksums are under
`db.test/backup-stream-2026-09-24-r1`.

## Authorized 60-minute backup attempt (2026-09-24)

After committing the socket-routing fix as `d350e4983`, a fresh profile CLI
from that clean revision ran the same 52-root `cdot` backup against the existing
primary. The user explicitly extended this backup's limit to 60 minutes;
the 45-second TERM-to-KILL grace remained unchanged. No builds/tests overlapped
measurement, and no daemon binary, service, repository policy, or source scope
changed. Before/after binary checksums matched.

The command reached the 60-minute cap and exited 124 after 3,601.117 seconds.
It still had not reached file processing. All seven goroutine captures at
60, 600, 1,200, 1,800, 2,400, 3,000 and 3,500 seconds show
`loadBackupParent -> LoadIndex -> DaemonEngine.loadBlobCatalog -> ScanPrefix`;
the final cancellation error identifies the same loader. Backup progress JSON
remained empty. Both inventories contain the identical 143 snapshot IDs, with
zero additional engine writes or commit requests. No new backup snapshot or
normalized backup metadata was published.

| Measurement | Value |
| --- | ---: |
| CLI user / system CPU, seconds | 181.00 / 31.22 |
| CLI average CPU cores | 0.059 |
| CLI peak RSS, KiB | 12,574,956 |
| Daemon status window, seconds | 3,601.877 |
| Daemon CPU, seconds / average cores | 2,656.97 / 0.738 |
| Sampled daemon peak RSS, KiB | 694,228 |
| Main-store GET requests | 3,307,923 |
| Main-store GET aggregate service, seconds | 3,122.508038 |
| Main-store GET-body bytes | 113,780,822,474 |
| Engine backpressure / L0-stall event deltas | 0 / 0 |
| Process / writer samples | 720 / 720 |
| Sampler errors / swaps | 0 / 0 |

The CPU profile contains 206.30 sampled CPU seconds across 3,599.98 seconds;
83.16% of sampled CPU is cumulatively below `loadBlobCatalog`, including map
operations, memory copying and protobuf decoding. These are CPU shares, not
wall-time shares. Low CLI utilization and repeated RPC-wait stack captures
identify serialized catalog fetching as the immediate startup bottleneck;
materializing the full catalog is also a growing memory cost (about 12 GiB peak).

Approximate ten-minute windows, computed from actual status timestamps:

| Window, minutes | GET/s | Logical body MiB/s | Daemon cores | CLI RSS at boundary, GiB |
| --- | ---: | ---: | ---: | ---: |
| 0-10 | 1,172 | 56.80 | 0.909 | 3.33 |
| 10-20 | 1,156 | 38.18 | 0.877 | 5.91 |
| 20-30 | 1,140 | 34.83 | 0.863 | 8.69 |
| 30-40 | 898 | 18.60 | 0.730 | 9.93 |
| 40-50 | 623 | 19.94 | 0.566 | 11.35 |
| 50-60 | 523 | 12.48 | 0.484 | 11.31 |

The falling logical read rate is observed, but completed catalog record counts
are unavailable: GET/s is not records/s or percent complete. No reliable ETA
can be inferred. Logical bodies do not establish physical disk/NFS traffic or
saturation; CPU and GET service durations overlap. This is one extended run,
not a matched comparison with the earlier ten-minute attempt.

Source inspection confirms each unary page constructs a new iterator and
collects one page. Backup uses `SchemaStore.ScanPrefix` without a transaction;
it does not use the existing transaction-scoped streaming scan with a retained
iterator and explicit 1 MiB read-ahead. The first candidate is to reuse that
bounded streaming path, preserving catalog validation, cancellation and lookup
projection correctness, and expose catalog progress. The exact costs of
iterator recreation, read-ahead and storage latency were not separately profiled
in the daemon. On-demand lookup is a separate, larger memory optimization;
simply removing `LoadIndex` remains unsafe while lookups require its projection.
Raising file-read concurrency cannot fix this pre-archiving phase.

The original primary remained PID 431717, read-write at epoch 55, with no active
transactions/intents or surviving backup process. No new-snapshot durability,
source traversal or payload throughput was measured. The command, committed
revision, hashes, profiles, interval summaries, five-second samples, inventories,
health snapshots, assertion-based analyzer and checksums are retained under
`db.test/authoritative-backup-60m-2026-09-24`.

## Authoritative backup startup (2026-09-24)

A real backup attempt used the saved `cdot` job's 52 readable source roots,
host `ncl1-1-ps`, paths grouping, one-filesystem traversal, and existing
exclusions. It used the primary HDD-NFS repository and daemon, with authoritative
metadata enabled and no deferred commit or metadata bypass. A profile CLI was
built from `63454280b` plus the recorded explicit-socket routing patch. No
build/test workload overlapped measurement; the daemon was not changed.

Preflight exposed that ordinary password unlock ignored the global
`--metadata-daemon-socket` option. A temporary socket alias was rejected by
endpoint permission checks and removed before backup. The CLI routing fix
honors the explicit socket when attaching the metadata engine, retaining all
endpoint validation. The encrypted native-daemon regression passed under the
race detector, including password unlock before storing a key-in-DB master key.

The backup reached the ten-minute TERM cap and exited 124 after 600.30 seconds.
It did not reach file processing or publish a snapshot. The before/after
inventories contain the same 143 snapshot IDs; daemon engine writes and commit
requests both increased by zero. The primary remained PID 431717, read-write
at epoch 55, with no active transactions/intents or remaining backup process.

| Measurement | Value |
| --- | ---: |
| CLI user / system CPU, seconds | 29.86 / 7.77 |
| CLI peak RSS, KiB | 2,321,456 |
| Daemon status window, seconds | 601.082 |
| Daemon CPU, seconds | 457.52 |
| Main-store GET requests | 504,245 |
| Main-store GET aggregate service, seconds | 531.009527 |
| Main-store GET-body bytes | 27,943,016,751 |

A non-disruptive goroutine capture at 60 seconds places startup in
`loadBackupParent -> LoadIndex -> DaemonEngine.loadBlobCatalog -> ScanPrefix`.
The cancellation error confirms that phase was still active at timeout.
The loader serially fetches 10,000-record `b:` pages and materializes every
location by pack before archiving. The CPU profile attributes 77.51% of its
36.42 sampled CPU seconds cumulatively to this loader; total CLI utilization
was only about 6% of one core. Daemon utilization averaged about 0.76 cores.

This identifies full-catalog preload as the immediate backup startup bottleneck,
not source traversal or file-read concurrency. Logical GET-body bytes do not
prove physical HDD/NFS saturation, and CPU/service durations overlap. No
archiving throughput or new-snapshot durability performance was measured.
The next candidate is bounded catalog streaming or context-aware on-demand
lookup; simply omitting `LoadIndex` is unsafe while lookup uses its projection.

The command, source patch, binary hashes, before/after inventories and status,
five-second samples, goroutine capture, CPU profile, reproducible analyzer,
resource timings and checksums are retained under
`db.test/authoritative-backup-2026-09-24`.

## Full index check after snapshot import (r54, 2026-09-24)

A fresh profile CLI from committed `3725db870` reran the full differential
index check against the primary after the 143 historical snapshot records were
imported. It used the existing HDD-NFS repository and scratch path, 32 workers
and RPC slots, 96 GiB memory/scratch limits, and a ten-minute TERM cap plus
45-second kill grace. No build/test workload overlapped the measurement.

The check completed in 305.21 seconds with exit status 0 and full coverage
marked complete. It detected 143 legacy snapshots and 143 SlateDB snapshots,
with zero snapshot mismatches, unresolved snapshots or snapshot-commit
mismatches. Historical records passed schema validation and their root IDs
resolved to catalog records containing tree locations. This verifies snapshot
membership, stored metadata and root catalog references, not recursive tree
traversal, pack payload integrity or a production restore.

Compared with pre-import r53, only two numeric result counters changed:

| Counter | r53 | r54 |
| --- | ---: | ---: |
| SlateDB snapshots | 0 | 143 |
| Snapshot mismatches | 143 | 0 |

Both sides still contain 379,934,385 blob locations. All location, pack,
aggregate and reference mismatch counters remain zero. The existing 419,530
warnings remain, along with 419,530 pending exports and unknown tier,
retention and usage-accounting pack counts. Exit status 0 is not a
warning-free verdict; this run did not enable `--fail-on-warning`.

CLI user/system CPU was 2,883.36/249.16 seconds, peak RSS was 82,149,368 KiB,
and peak scratch use was 39,886,207,390 bytes, with zero swaps. Wall time is
close to r53's 304.50 seconds, but these are not matched performance repeats.
The primary remained PID 431717, read-write at epoch 55, with no active
transactions/intents and zero additional engine writes. Scratch was empty
after completion; CLI and daemon executable identities were verified.

The command, build identity, full JSON verdict, progress/telemetry, resource
samples, before/after health, reproducible analyzer and checksums are retained
under `db.test/phase33-production-2026-09-24-stream32-historical-snapshots-full-r54`.

## Snapshot metadata-only import (2026-09-24)

A fresh profile CLI built from `3f0544b9f` imported all 143 historical snapshot
records into the existing primary using `--snapshot-metadata-only --resume`.
The repository and database remained on HDD-backed NFS. The run retained the
ten-minute TERM cap plus 45-second grace and stopped on the first source error.
No build/test workload overlapped measurement, and no daemon installation,
restart, reset, activation, or authority change occurred.

| Measurement | Publication | Dry-run resume verification |
| --- | ---: | ---: |
| Exit status | 0 | 0 |
| Wall time, seconds | 15.34 | 2.34 |
| CLI user / system CPU, seconds | 1.98 / 0.26 | 0.53 / 0.11 |
| Peak CLI RSS, KiB | 209,728 | 176,308 |
| Snapshots seen | 143 | 143 |
| Snapshots imported / resumed | 143 / 0 | 0 / 143 |
| Daemon engine write-operation delta | 143 | 0 |

Both commands reported zero errors, warnings, trees/nodes visited, imported
packs/blobs, and crawl debt. The resume check revalidated matching stored JSON
and root locations without additional writes. These are preserved historical
snapshot records, not verified inode identities or full tree/data verification;
no traversal checkpoints were created by this mode. Full traversal still uses
the old catalog preload. The earlier 600-second timeout and this run therefore
have different scopes and do not establish a like-for-like full-import speedup.

The publication daemon-status window was 16.307 seconds, including setup and
final collection, with 5.65 daemon CPU seconds. There were 143 commit requests
with 11.115136 aggregate service seconds, including 143 durable waits totaling
10.694801 seconds. Admission lock hold totaled 11.929311 seconds but admission
wait was only 0.000079 seconds. These nested timings overlap; they must not be
added or interpreted as independent wall-clock phases. They point to per-record
durability, not CPU or admission contention, as the next measured optimization
target. Bounded transactional snapshot batching merits evaluation while retaining
root validation, immutability, durable publication and retry semantics.

The same window recorded 786 main-store GETs, 2.632275 aggregate GET service
seconds, and 1,794,691,842 returned body bytes, plus 286 WAL PUTs totaling
194,667 bytes. These are logical object-store counters, not physical NFS I/O.
The resume check has different work and warmer caches and is not a matched
performance control. No full differential checker or payload restore was run
as part of this measurement.

The primary remained PID 431717, read-write at epoch 55, with zero active
transactions/intents after publication and resume. Binary hashes were unchanged
through the run. Commands, committed candidate, timings, one-second telemetry,
before/after writer status, reproducible `analyze.cjs`/`analysis.json`, and
checksums are retained under `db.test/snapshot-metadata-only-2026-09-24`.

## Historical snapshot import startup observation (2026-09-24)

After historical snapshot support was committed as `c23b12067`, the user
authorized a resumable import into the existing primary database and requested
performance observation. A fresh committed CLI used the existing socket and
HDD-NFS repository, with unlimited snapshot depth, resume enabled, a one-source
error stop and a ten-minute TERM cap plus 45-second kill grace. No reset,
activation, service installation or restart was requested or performed.

Cancelling the terminal tool did not stop its child import. Observation attached
to the surviving PID 917005 rather than launching a second importer. The import
then reached its original cap: the recorded lifecycle is 600.038 seconds and
the error is `load source indexes for snapshot import: load authoritative blob
catalog: rpc error: code = Canceled desc = context canceled`. Snapshot traversal
was never reached. All import result counts are zero, including snapshots and
nodes; daemon engine write operations did not increase. The 143 production
snapshots therefore remain unimported.

The five-second sampler captured 52 live-process observations over the final
255.251 seconds. These are not whole-run resource totals:

| Measurement | Import CLI | VaulticDB |
| --- | ---: | ---: |
| Sample-window CPU seconds | 15.87 | 191.91 |
| Mean CPU cores used | 0.062 | 0.752 |
| First / last RSS, KiB | 1,520,712 / 2,386,812 | 598,252 / 601,596 |
| Sampled peak RSS, KiB | 2,386,812 | 636,044 |
| Logical `rchar` delta, bytes | 753,504,915 | 67,003,384,804 |

The wider 632.583-second daemon-status window includes setup and post-run
observation. It records 459.76 daemon CPU seconds, 506,756 main-store GETs,
532.447 aggregate GET service seconds and 28,018,033,665 returned body bytes.
GET counts and aggregate service are not equivalent to physical NFS reads or
exclusive wall-clock disk wait. Kernel thread samples mostly show futex waits,
with some daemon NFS waits; they are not Go goroutine or Rust async stack
profiles. No CPU stack profile was collected, so exact crypto/iterator/I/O
shares remain unmeasured. GNU time output and the shell timeout exit status
were lost with the cancelled wrapper; lifecycle/error output establishes the
cap, not an invented exit-code observation.

The controlling path is `runIndexImport` calling `repo.LoadIndex` before
`legacyimport.Import`. `DaemonEngine.loadBlobCatalog` serially scans all `b:`
records through 10,000-record unary `ScanPrefix` pages and accumulates all blob
locations by pack. It does not use the checker's parallel streaming scan.
This is the first demonstrated bottleneck, before snapshot publication or
durability can be measured. The next optimization should remove unnecessary
full-catalog materialization for snapshot tree loading, or replace this unary
startup scan with bounded streaming. Increasing pack import workers cannot
parallelize this earlier code path. No optimization was applied in this run.

There are 41 monitor snapshots, with the initial daemon collection unavailable.
The saved analyzer accounts for that missing collection and checks zero imported
snapshots/nodes and healthy writer state. The primary remains read-write at
epoch 55, with zero active transactions/intents and no import process left.
Logs, committed executable, source/binary identities, process/NFS samples and
reproducible analysis are retained under
`db.test/historical-snapshot-import-2026-09-24`.

## Full-check catalog streaming (r53, 2026-09-23)

The matched block-scratch evidence was committed as `212cfba1c`, without
pushing. This working-tree candidate enables full-check pack catalog streaming
only when the checker store exposes its RPC limiter with more than one slot.
A stream holds one slot while its callback may issue a missing-pack `Get`;
single-slot and unknown stores therefore retain pagination. SlateDB-only
streaming is unchanged. No RPC, memory or scratch limit is raised, and no
missing-pack verification is skipped or moved outside admission accounting.

Tests exercise actual one-slot/two-slot admission, exact catalog results and
aggregates, missing contributions before and after scanned packs, omitted
existing-pack detection, cancellation and released permits. Existing pagination,
malformed-record and checker tests pass. Full maintenance race tests (9.890s),
affected CLI `Test(Check|Index)` race tests (20.727s), formatting, editor
diagnostics and isolated profile build passed before the diagnostic. The first
new test compile required a missing standard-library import; its rerun passed.

R53 used the same HDD-NFS repository and scratch, 32 loader workers/RPCs,
at most four comparison tasks, 96 GiB memory/scratch budgets and ten-minute
TERM cap with 45-second kill grace. No build/test workload overlapped it.
There was no daemon installation, restart or cache reset.

| Measurement | Prior block r50/r51 | r53 streaming |
| --- | ---: | ---: |
| Wall seconds including cleanup | 324.75 / 321.67 | 304.50 |
| CLI CPU seconds | 3,202.00 / 3,140.33 | 3,117.63 |
| Legacy scan | 75s / 72s | 75s |
| Encryption audit | 14s / 14s | 14s |
| SlateDB scan | 70s / 70s | 70s |
| SlateDB finalization | 9s / 9s | 10s |
| Catalog join | 41s / 43s | 23s |
| Partitioned merge plus comparison | 84s / 82s | 80s |
| Peak RSS, KiB | 81,942,052 / 82,213,332 | 80,545,028 |
| Peak scratch bytes | 39,874,827,674 / 39,902,974,248 | 39,871,140,044 |
| Recorded scratch read/write bytes | 104,546,699,652 / 104,544,992,400 | 104,546,041,540 |
| Completed merge groups | 96 / 95 | 94 |

Against the prior two-run mean, catalog time decreased 45.2%, elapsed time
5.8% and CLI CPU 1.7%. Scratch traffic is effectively unchanged, with zero
swaps. This is one candidate observation against earlier controls, not a
fresh matched sequence or repeatable speedup acceptance. Cache state and
run variability remain confounders; no cold-cache or native RADOS claim is made.

Exact logical results match both controls excluding resource telemetry,
consistency session ID and separately recorded encrypted objects (161 in all
three). Both sides contain 379,934,385 locations, with zero location, pack,
aggregate or reference mismatches. Full scope completed but is not clean:
143 missing SlateDB snapshots and 419,530 warnings/pending exports remain.
Checker exit 2 is the same metadata-difference verdict; harness exit 1 is its
known final accepted-exit gate. No metadata repair was performed.

Per-partition blob counts/chunks/ranges match across all 256 partitions.
Aggregate stream telemetry now also includes the 419,530 pack records in
42 chunks: 376,766,240 records, 37,811 chunks and 257 completed ranges total.
Reported stream bytes are 35,623,707,627. The added catalog range must not be
interpreted as expanded blob coverage or compared blindly with blob-only
aggregate telemetry from the controls.

The saved analyzer checks exact logical parity, unchanged limits, blob scope,
expected catalog telemetry, exit codes, cleanup, zero swaps and daemon health.
Artifacts, executable, tested patch, validation logs and comparison are under
`db.test/phase33-full-catalog-stream-2026-09-23`; raw output is under
`db.test/phase33-production-2026-09-23-stream32-full-catalog-stream-full-r53`.
The measured CLI SHA256 is
`f33983e390ca2a0fd82c4f34ebd0a3df6f71d38e321cc58d187dba51c2191bf1`.
Raw and supplemental manifests verified, scratch is empty, and the adopted
daemon remains PID 431717, epoch 55, read-write with zero transactions/intents
and unchanged executable. Source, tests and this evidence remain uncommitted.

## Block scratch matched repeats (r49-r52, 2026-09-23)

The block-scratch implementation, tests and initial r48 evidence were committed
as `e008f4c01` with a detailed message, without pushing. A fresh
control/block/block/control sequence reused the exact saved partitioned
per-record and block-scratch executables. No rebuild, daemon restart, cache reset
or installation occurred. Paired executable hashes match; current workspace
revisions in run metadata are not the source identity of the saved binaries.

All four runs used HDD-NFS data and scratch, 32 loader workers/RPCs, at most four
comparison tasks, unchanged 96 GiB checker memory/scratch limits and separate
ten-minute TERM caps with 45-second kill grace. Runs were sequential with no
overlapping builds/tests or detected backup workloads. The wrapper stopped on
unexpected results, cleanup failures or daemon changes; all four completed its
known-exit-2, logical parity, scan scope, limits and operational health gates.

| Measurement | r49 control | r50 block | r51 block | r52 control |
| --- | ---: | ---: | ---: | ---: |
| Wall seconds including cleanup | 375.80 | 324.75 | 321.67 | 370.15 |
| CLI CPU seconds | 3,518.38 | 3,202.00 | 3,140.33 | 3,507.70 |
| Legacy scan | 81s | 75s | 72s | 79s |
| Encryption audit | 14s | 14s | 14s | 14s |
| SlateDB scan | 74s | 70s | 70s | 73s |
| SlateDB finalization | 13s | 9s | 9s | 14s |
| Catalog join | 46s | 41s | 43s | 42s |
| Partitioned merge plus comparison | 115s | 84s | 82s | 116s |
| Peak RSS, KiB | 90,979,164 | 81,942,052 | 82,213,332 | 89,455,116 |
| Peak scratch bytes | 48,720,798,086 | 39,874,827,674 | 39,902,974,248 | 48,714,978,284 |
| Recorded scratch read/write bytes | 127,723,817,300 | 104,546,699,652 | 104,544,992,400 | 127,723,722,760 |
| Completed merge groups | 96 | 96 | 95 | 95 |

Both block runs finished faster than both controls and used fewer CLI CPU
seconds. Mean elapsed time decreased from 372.975 to 323.210 seconds (13.3%),
CLI CPU from 3,513.040 to 3,171.165 seconds (9.7%), and merge/comparison from
115.5 to 83 seconds (28.1%). Mean peak RSS decreased 9.0%, peak scratch 18.1%
and recorded scratch traffic 18.1%. All runs had zero swaps. The scratch traffic
counter combines reads and writes; it is not a physical disk-byte measurement.

Complete logical results match across all four runs after excluding resource
telemetry, consistency session ID and the separately recorded live
encrypted-object count, which was 161 throughout. This includes retained
findings, inventory/options digests and all 379,934,385 locations on each side.
Location, pack, aggregate and reference mismatches remain zero. Every run exits
2 for the same 143 missing SlateDB snapshots and retains 419,530 warnings and
pending exports. Harness exit 1 is its known final accepted-exit gate, not a
crash. No metadata repair was performed and clean full-check acceptance remains
blocked by the snapshot discrepancies.

Configured limits and scan scope match exactly: 10,019 legacy indexes,
376,346,710 SlateDB records, 37,769 chunks and 256 ranges. Scan byte totals vary
slightly (35,556,163,085 / 35,556,163,083 / 35,556,163,092 / 35,556,163,084).
Two observations per variant support the measured NFS elapsed/CPU/I/O improvement
but provide no confidence bounds or cold-cache, other-workload or native RADOS
acceptance. No deployment decision is implied.

All four raw manifests verified, and the saved analysis reproduces exactly,
including paired binary identities, logical parity, scope, limits and health.
Final live checks confirmed empty scratch and unchanged PID 431717, epoch 55,
read-write with zero transactions/intents and the adopted executable unchanged.
Runner, analyzer, comparison JSON, copied binaries and logs are under
`db.test/phase33-block-repeats-2026-09-23`. Raw runs are
`db.test/phase33-production-2026-09-23-stream32-block-control-full-r49`,
`db.test/phase33-production-2026-09-23-stream32-block-candidate-full-r50`,
`db.test/phase33-production-2026-09-23-stream32-block-candidate-full-r51` and
`db.test/phase33-production-2026-09-23-stream32-block-control-full-r52`.

## Block-authenticated scratch (r48, 2026-09-23)

The repeat evidence was committed as `aa677ab5b`, without pushing. The next
working-tree candidate batches up to 512 location tuples into one authenticated
scratch frame instead of one frame per tuple. Temporary run magic changes from
`VLTCHK01` to `VLTCHK02`; repository and daemon formats are unchanged. Scratch
runs are session-owned and are not resumed across binaries. Each frame retains
AES-GCM authentication with the run prefix and sequential block nonce, with
strict positive, aligned and bounded plaintext lengths.

Both direct spill and streaming merge writers construct and encrypt blocks in
the existing 1 MiB buffered output allocation. Readers authenticate a complete
block in their existing input buffer before exposing any of its tuples. No
additional per-reader block allocation or tuple-budget increase is introduced.
Readers also check framing against the trusted in-memory run size, rejecting
whole-block removal and trailing frames as well as partial truncation. This
does not add a separately authenticated EOF footer. Empty runs contain only
the header. Direct reservations account for tuple bytes plus per-block framing;
merge reservations remain bounded by input payload sizes, with unused capacity
released on close and all reserved bytes released on abort.

The first focused check exposed the intermediate mismatch between block-sized
direct writes and old per-tuple merge framing. Batching merge output resolved
the reservation failures, and the merge-headroom fixture now uses actual input
sizes. Block tests cover empty, partial, full, boundary-crossing and multi-buffer
runs for both writer paths, exact tuples and file sizes, surplus reservations,
abort cleanup and writes after close. Reader tests cover refill boundaries,
length/alignment tampering, ciphertext/tag corruption, replay, whole-block
removal and representative partial-frame truncations. Authentication failures
are rejected before returning the affected block's first tuple. Concurrent merge
admission, cancellation, corruption and cleanup tests also pass.

Full maintenance race tests (9.301s), affected CLI `Test(Check|Index)` race tests
(20.445s), formatting, editor diagnostics and isolated profile build passed.
No builds/tests overlapped the HDD-NFS diagnostic. R48 retained 32 loader
workers/RPCs, at most four comparison tasks, 96 GiB checker memory/scratch limits,
the ten-minute TERM cap with 45-second kill grace and the unchanged adopted
daemon without restart or installation.

| Measurement | Prior partitioned r45/r46 | r48 block scratch |
| --- | ---: | ---: |
| Wall seconds including cleanup | 372.62 / 405.09 | 325.26 |
| CLI CPU seconds | 3,505.92 / 3,514.54 | 3,120.18 |
| Merge plus comparison | 113s / 116s | 83s |
| Peak RSS, KiB | 91,338,172 / 87,492,952 | 81,184,508 |
| Peak scratch bytes | 48,700,116,290 / 48,722,914,994 | 39,885,561,628 |
| Recorded scratch read/write bytes | 127,723,817,060 / 127,723,816,892 | 104,543,545,952 |
| Completed merge groups | 96 / 96 | 91 |

R48's rounded stages were legacy scan 73s, audit 14s, SlateDB scan 69s,
finalization 11s, catalog join 45s, partitioned merge/comparison 83s, location
cleanup 4s and parallel validation 16s. Finalization began at 315 seconds.
Relative to the prior two-run mean, observed elapsed time is 16.4% lower, CLI CPU
11.1% lower, scratch traffic 18.1% lower and peak scratch 18.1% lower. These are
one candidate run against prior runs, not matched repeats; the figures are
promising evidence, not a stable speedup or CPU-saving guarantee. The number of
merge groups also changed with runtime spill distribution.

Complete logical results match r45 after excluding only resources, session ID
and the live encrypted-object count (161 in both runs). Configured limits and
scan records/chunks/ranges match exactly: 10,019 legacy indexes, 376,346,710
SlateDB records, 37,769 chunks and 256 ranges. R48 records 35,556,163,090 scan
bytes and zero swaps. All 379,934,385 locations on each side match, with zero
location, pack, aggregate or reference mismatches. Exit remains 2 for the same
143 missing SlateDB snapshots and 419,530 warnings/pending exports; the harness
then returns 1 at its known accepted-exit gate. No snapshot repair was attempted.

The measured source matches the saved candidate patch. Raw checksums, empty
scratch and final live health checks passed. PID 431717 remained read-write at
epoch 55 with zero transactions/intents and the adopted executable unchanged.
The candidate remains uncommitted and is not installed. Matched repeats, clean
snapshot acceptance and native RADOS acceptance remain open. Artifacts are under
`db.test/phase33-block-scratch-2026-09-23` and
`db.test/phase33-production-2026-09-23-stream32-block-scratch-full-r48`.

## Partitioned comparison repeats (r44-r47, 2026-09-23)

The partitioned implementation and r45 evidence were committed as `f7cb4252a`
with a detailed message, without pushing. A candidate repeat (r46) followed by a
control repeat (r47) extended r44/r45 into a control/candidate/candidate/control
sequence. Each variant reused its exact saved executable; no rebuild, daemon
restart or installation occurred. Binary identity was verified within each pair.
The run metadata records the current workspace revision, while the executable
hashes and original candidate patches identify the actual tested sources.

Every run used HDD-NFS repository and scratch, 32 loader workers/RPCs, unchanged
96 GiB checker memory/scratch limits and a ten-minute TERM cap with 45-second
kill grace. The candidate uses at most four comparison tasks. Runs were strictly
sequential, without overlapping builds/tests or detected backup workloads. The
repeat wrapper verifies the known exit-2 result, complete logical parity, scan
scope, limits, scratch cleanup, daemon identity and health before continuing.
It does not treat the harness's final exit-1 gate as a successful checker verdict.

| Measurement | r44 control | r45 candidate | r46 candidate | r47 control |
| --- | ---: | ---: | ---: | ---: |
| Wall seconds including cleanup | 578.22 | 372.62 | 405.09 | 594.24 |
| Merge preparation plus comparison | 330s | 113s | 116s | 333s |
| Encryption audit | 14s | 14s | 14s | 14s |
| SlateDB scan | 72s | 72s | 75s | 73s |
| Catalog join | 42s | 47s | 56s | 50s |
| Parallel validation | 20s | 18s | 35s | 22s |
| CLI CPU seconds | 3,513.95 | 3,505.92 | 3,514.54 | 3,523.04 |
| Peak RSS, KiB | 92,798,392 | 91,338,172 | 87,492,952 | 90,924,088 |
| Peak scratch bytes | 55,711,066,520 | 48,700,116,290 | 48,722,914,994 | 55,683,485,230 |
| Recorded scratch read/write bytes | 122,802,430,740 | 127,723,817,060 | 127,723,816,892 | 122,802,430,740 |
| Completed merge groups | 7 | 96 | 96 | 7 |

Both candidates finished faster than both controls. Mean elapsed time decreased
from 586.23 to 388.855 seconds (33.7%), and mean merge/comparison time from 331.5
to 114.5 seconds (65.5%). Mean peak scratch decreased 12.5%, while recorded
scratch traffic increased 4.0%. Mean CLI CPU changed only from 3,518.495 to
3,510.23 seconds (0.23% lower); this is a parallelism benefit, not evidence of
material CPU savings. Mean peak RSS was 2.7% lower and all runs had zero swaps.
The slower candidate repeat spent more time in catalog and parallel validation;
its merge/comparison stage remained close to the first candidate.

All four completed checks exit 2 with matching logical results, including all
retained findings, inventory/options digests and 379,934,385 locations on each
side. Location, pack, aggregate and reference mismatch counters remain zero.
The same 143 missing SlateDB snapshots and 419,530 warnings/pending exports
remain; no metadata repair was attempted. Logical comparison excludes resource
telemetry, session ID and the live encrypted-object count. The latter was
recorded separately and was 161 in every run. Scan records/chunks/ranges and
configured limits match exactly; scan byte totals vary slightly. R46/r47 record
35,556,163,098 and 35,556,163,090 bytes respectively.

The reverse-order repeat supports the elapsed-time improvement under this NFS
configuration. There are only two observations per variant, with no confidence
bounds, cache reset or daemon restart; this does not establish cold-cache
performance, other workloads or native RADOS acceptance. Snapshot discrepancies
still prevent clean full-check acceptance. No deployment decision is implied.

All four raw manifests verified, and the reproducible analysis checks paired CLI
hashes, logical equality, scope, limits, cleanup and daemon health. Final live
checks confirmed empty scratch and the unchanged adopted executable at PID
431717, epoch 55, read-write with zero transactions/intents. The repeat runner,
analysis, copied executables, logs and comparison JSON are under
`db.test/phase33-partition-repeats-2026-09-23`. New raw runs are under
`db.test/phase33-production-2026-09-23-stream32-partition-repeat-full-r46` and
`db.test/phase33-production-2026-09-23-stream32-buffered-repeat-full-r47`.

## Bounded partitioned comparison (r45, 2026-09-23)

After buffered-reader commit `2d3950da5`, the next working-tree candidate routes
legacy locations into 16 ordered blob-ID partitions. Worker and partition tuple
allocations divide the existing legacy spool budget. Each partition uses chunks
of at most one eighth of its allocation so overflow retains other sorted memory
runs. Parent adoption accounts for child capacity and transfers run ownership.
Full checks enable partitioning only when each requested worker/partition can
receive at least 1 MiB; smaller budgets retain the existing serial path. Legacy
worker count is also clamped by per-partition tuple capacity.

Comparison aligns each legacy partition with 16 of SlateDB's existing 256 prefix
partitions. Up to four tasks run concurrently, further limited by the existing
memory allocation and merge-reader buffer sizing. Necessary within-partition
merges execute inside these tasks; there is no global legacy merge. Tasks retain
separate results and combine counters and bounded findings deterministically.
Errors cancel sibling tasks, parent contexts are restored, and cleanup retains
the original spool ownership. Exact tuple comparison, deduplication, pack
contributions, scratch encryption and configured resource limits are unchanged.
The `location_compare_partitioned` stage includes both merge preparation and
comparison; partial counters are combined when the tasks finish, not live.

Extended loader tests compare partitioned output with a serial global reference
across budgets, worker counts and optional pack contributions. New concurrent
comparison tests cover duplicates, asymmetric records, tuple differences, existing
findings, unlimited and bounded findings, cancellation, corrupted encrypted runs,
context restoration and cleanup. Focused tests, full maintenance race tests
(9.712s), affected CLI `Test(Check|Index)` race tests (21.669s), formatting,
editor diagnostics and the isolated profile build passed.

R45 used HDD-NFS data and scratch, the same adopted daemon without restart,
32 loader workers/RPCs, at most four comparison tasks, unchanged 96 GiB checker
memory/scratch limits and the ten-minute TERM cap with 45-second kill grace.
No builds or tests overlapped measurement. Rounded progress boundaries:

| Measurement | r44 buffered reader | r45 partitioned comparison |
| --- | ---: | ---: |
| Legacy scan | 73s | 79s |
| Encryption audit | 14s | 14s |
| SlateDB scan | 72s | 72s |
| SlateDB finalization | 15s | 14s |
| Catalog join | 42s | 47s |
| Location merge preparation plus comparison | 330s | 113s |
| Location cleanup starts | 546s | 339s |
| Finalization starts | 572s | 364s |
| Wall time including cleanup | 578.22s | 372.62s |
| CLI CPU seconds | 3,513.95 | 3,505.92 |
| Peak RSS, KiB | 92,798,392 | 91,338,172 |
| Peak scratch bytes | 55,711,066,520 | 48,700,116,290 |
| Recorded scratch read/write bytes | 122,802,430,740 | 127,723,817,060 |
| Completed merge groups | 7 | 96 |

The complete logical results match exactly after excluding only resources and
the consistency session ID. This includes all retained findings, encryption
counts, legacy inventory digest and options digest. Both sides contain exactly
379,934,385 locations with zero location, pack, aggregate or reference mismatches.
Both completed checks exit 2 for the same 143 missing SlateDB snapshots; 419,530
warnings and pending exports remain. This is not clean full-check acceptance.
The unchanged harness exits 1 at its final accepted-exit gate after saving and
validating the run artifacts; no metadata repair was attempted.

Both scans cover 10,019 legacy indexes and 376,346,710 SlateDB records in 37,769
chunks across 256 ranges; r45 records 35,556,163,085 scan bytes and zero swaps.
Observed elapsed time is 35.6% lower and peak scratch 12.6% lower, while scratch
traffic is 4.0% higher and CLI CPU is essentially unchanged (0.23% lower).
The larger number of smaller merge groups is not a reduction in aggregate work.
These are single sequential completed runs, not matched repeated trials; no
confidence bounds or stable speedup claim follow from this pair. The result
supports retaining the candidate for repeat testing, not deployment acceptance.

The measured source matches the saved candidate patch. Raw checksums, empty
scratch and final health checks passed. PID 431717 remained read-write at epoch
55 with zero transactions/intents and the adopted executable unchanged. The
candidate is uncommitted and not installed. Matched repeats, snapshot discrepancy
resolution and native RADOS acceptance remain outstanding. Artifacts are under
`db.test/phase33-parallel-compare-2026-09-23` and
`db.test/phase33-production-2026-09-23-stream32-parallel-compare-full-r45`.

## Buffered scratch reader (r44, 2026-09-23)

The legacy-summary implementation and r43 evidence were committed as
`366a9532f` with a detailed message, without pushing. The next working-tree
candidate removes two per-record copies from encrypted scratch reads. It peeks
at the length and complete record in the existing buffered reader, authenticates
and decrypts in place, decodes the tuple, then discards the consumed bytes.
The scratch format, AES-GCM authentication, nonce sequence, length validation
and partial-record error semantics remain unchanged. No resource limit changed.

Focused tests passed, including a 12,000-record fixture crossing buffer refills
with 110/111/219-byte and production 1 MiB buffers, all 109 partial-record lengths,
repeated EOF, invalid length, corrupted authentication tag and replay rejection.
Full maintenance race tests and affected CLI `Test(Check|Index)` race tests,
formatting, editor diagnostics and isolated profile build passed. No builds or
tests overlapped the measurement.

R44 used the same adopted daemon without restart, HDD-NFS repository and scratch,
32 workers/RPCs, 96 GiB checker memory/scratch limits and ten-minute TERM cap with
45-second kill grace. Rounded progress boundaries:

| Measurement | r43 legacy summaries | r44 buffered reader |
| --- | ---: | ---: |
| Legacy scan | 74s | 73s |
| Encryption audit | 35s | 14s |
| SlateDB scan | 73s | 72s |
| SlateDB finalization | 17s | 15s |
| Catalog join | 43s | 42s |
| Legacy merge preparation | 56s | 53s |
| SlateDB merge preparation | <1s | <1s |
| Comparison starts | 298s | 269s |
| Comparison duration | 302s, interrupted | 277s, completed |
| Legacy locations | 378,503,112, partial | 379,934,385 |
| SlateDB locations | 378,503,111, partial | 379,934,385 |
| Completed merge groups | 7 | 7 |
| Recorded scratch read/write bytes | 122,633,354,410 | 122,802,430,740 |
| Peak scratch bytes | 55,679,304,826 | 55,711,066,520 |
| CLI CPU seconds | 3,541.78 | 3,513.95 |
| Peak RSS, KiB | 92,782,444 | 92,798,392 |
| Wall time including cleanup | 611.78s | 578.22s |
| CLI exit | 130, wrapper timeout 124 | 2, metadata indexes differ |

R44 completed location comparison at 546 seconds, location cleanup at 552 seconds,
and parallel validation at 572 seconds before finalization. All 379,934,385
locations matched exactly. Missing/invalid packs, aggregate mismatches, reverse
edge mismatches and unresolved references were zero. The final result reported
143 legacy snapshots, zero SlateDB snapshots and 143 snapshot mismatches; its
100 retained findings were all `missing_snapshot`, wanting `slatedb`. The
snapshot comparator reports this category when neither a matching snapshot nor
an import checkpoint is present. This run does not establish when that state
arose, and no metadata repair was attempted. The result also retains 419,530
pending exports and warnings, including unknown-tier, retention and usage counts.

The check finished before the cap but did not pass. The harness subsequently
returned 1 because its accepted exits are only 0 and 124; the checker exit was
2, not an execution crash. Raw artifacts, post-run health and cleanup had already
been saved and verified before that harness exit gate. This is a completed
differential result, not clean full-check acceptance.

The run scanned the same 10,019 legacy indexes and 376,346,710 SlateDB records in
37,769 chunks across 256 ranges, recording 35,556,163,091 scan bytes and zero swaps.
Comparison completed in less time than r43's interrupted comparison, but these
are single sequential runs with different work boundaries. Audit alone was
21 seconds shorter. No precise completed-check speedup or total CPU-saving claim
is justified without matched repeats. Native RADOS acceptance remains open.

The measured Go patch matches the working source. Raw checksums, empty scratch
and final health checks passed. The daemon remained PID 431717, epoch 55,
read-write with zero transactions/intents and the adopted executable unchanged.
The reader candidate remains uncommitted and is not installed. Artifacts are
under `db.test/phase33-buffered-reader-2026-09-23` and
`db.test/phase33-production-2026-09-23-stream32-buffered-reader-full-r44`.

## Bounded legacy pack summaries (r43, 2026-09-23)

The ordered-partition implementation and r41/r42 evidence were committed as
`8e23554fe` with a detailed message, without pushing. The next working-tree
candidate enables the existing pack-summary representation for legacy catalog
contributions. Each legacy worker reserves half of its pack-spool allocation
for a bounded aggregation map when the budget permits, flushing partial
summaries into the other half. Tiny budgets retain direct summary insertion.
Finalization flushes and releases the maps before adopting worker spools.
Exact location tuples and pack-presence markers remain unchanged; duplicate
contributions retain their counts, payload totals and type information. Scratch
encryption and total configured memory/scratch budgets are unchanged.

New regression tests compare raw and summarized pack streams with one-tuple,
eight-tuple and 1 MiB budgets and requested worker counts 1/4/32. They verify
duplicate multiplicity, payload totals, pack presence, exact location counts,
bounded retained tuple capacity and cleanup. Existing loader, catalog and
pack-contribution tests passed, as did the full maintenance race suite and
affected CLI `Test(Check|Index)` race tests. Formatting, editor diagnostics and
the isolated profile build passed. The previously documented unrelated full
CLI failures were not rerun or modified.

R43 used the same adopted daemon without restart, HDD-NFS repository and scratch,
32 workers/RPCs, 96 GiB checker memory/scratch limits and ten-minute TERM cap with
45-second kill grace. Builds and tests completed before measurement. Progress
boundaries are rounded:

| Measurement | r42 ordered partitions | r43 legacy summaries |
| --- | ---: | ---: |
| Legacy scan | 101s | 74s |
| Encryption audit | 42s | 35s |
| SlateDB scan | 74s | 73s |
| SlateDB finalization | 20s | 17s |
| Catalog join | 179s | 43s |
| Legacy merge preparation | 68s | 56s |
| SlateDB merge preparation | <1s | <1s |
| Comparison starts | 484s | 298s |
| Comparison window before cap | 116s | 302s |
| Partial legacy locations | 140,963,015 | 378,503,112 |
| Partial SlateDB locations | 140,963,014 | 378,503,111 |
| Completed merge groups | 14 | 7 |
| Recorded scratch read/write bytes | 152,533,184,512 | 122,633,354,410 |
| Peak scratch bytes | 70,198,433,856 | 55,679,304,826 |
| CLI CPU seconds | 3,906.20 | 3,541.78 |
| Peak RSS, KiB | 122,980,788 | 92,782,444 |
| Wall time including cleanup | 616.18s | 611.78s |

Both runs scanned 10,019 legacy indexes and 376,346,710 SlateDB records in
37,769 chunks across 256 ranges. R43 recorded 35,556,163,089 scan bytes. It exited
124 with CLI cancellation 130 and zero swaps. The one-location partial-count
difference can occur when cancellation interrupts iterator advancement; zero
partial mismatch counters are not a completed correctness verdict.

Catalog joining took 136 seconds less in this run, with seven fewer merge groups,
19.6% less recorded cumulative scratch traffic and 20.7% lower peak scratch,
despite substantially more comparison work. Peak RSS was 24.6% lower. The memory
limit is a tuple/aggregation budget, not an RSS cap. This supports the bounded
summary approach, but sequential single capped runs are not matched repeated
full-check measurements. Audit time also varied, and partial work boundaries
differ. The CPU figures do not establish total completed-check CPU savings.

Raw checksums and empty scratch cleanup passed. The daemon remained PID 431717,
epoch 55, read-write with zero transactions/intents and its adopted executable
unchanged. The candidate remains uncommitted and is not installed. A completed
differential verdict and matched repeats remain open; native RADOS acceptance
is also outstanding. The remaining measured tail is legacy merge preparation
and exact comparison, which should guide the next bounded optimization.

Artifacts are under `db.test/phase33-legacy-summaries-2026-09-23` and
`db.test/phase33-production-2026-09-23-stream32-legacy-summaries-full-r43`.

## Preserve ordered SlateDB partitions (r42, 2026-09-23)

The r41 attribution identified 95 seconds preparing the global SlateDB location
iterator. The next candidate retains the 256 disjoint blob-ID partition spools
and concatenates their sorted iterators in prefix order, opening one partition
at a time. Full-mode scans now validate blob key kind and partition membership
before accepting a record. Memory capacity remains charged to the parent spool;
partition ownership, cancellation and cleanup follow the existing parent
context. Legacy spools, pack contributions, exact comparison and the configured
resource limits are unchanged. Any necessary within-partition merges execute
lazily during comparison rather than being hidden as eliminated work.

Tests compare spilled partitions with a global reference spool, including
duplicates, empty and widely separated prefixes, exact counts, cancellation,
resource cleanup and rejection of out-of-partition keys. The fixture verifies
that no global merge is performed. Focused race tests and the full maintenance
race suite passed. The full CLI suite failed in backup permission expectations,
Phase 34 scan-response result equality, monitor snapshot golden output and the
default version string. Each failure category was reproduced on clean HEAD
`326450df5`; the Phase 34 reproduction used the same existing test daemon and
the `0ms/repeat-1` case. These failures were not changed by this experiment.

R42 used the same adopted daemon without restart, HDD-NFS repository and scratch,
32 workers/RPCs, 96 GiB memory/scratch limits and ten-minute cap as r41. Builds
and tests finished before measurement. Rounded progress boundaries:

| Measurement | r41 attribution control | r42 ordered partitions |
| --- | ---: | ---: |
| Legacy scan | 98s | 101s |
| Encryption audit | 56s | 42s |
| SlateDB scan | 75s | 74s |
| SlateDB finalization | 16s | 20s |
| Catalog join | 183s | 179s |
| Legacy merge preparation | 69s | 68s |
| SlateDB merge preparation | 95s | <1s |
| Comparison starts | 592s | 484s |
| Comparison window before cap | 8s | 116s |
| Partial legacy locations | 7,158,424 | 140,963,015 |
| Partial SlateDB locations | 7,158,424 | 140,963,014 |
| Completed merge groups | 30 | 14 |
| Recorded scratch read/write bytes | 200,006,112,552 | 152,533,184,512 |
| Peak scratch bytes | 70,240,992,196 | 70,198,433,856 |
| CLI CPU seconds | 3,908.50 | 3,906.20 |
| Peak RSS, KiB | 119,816,020 | 122,980,788 |
| Wall time including cleanup | 613.26s | 616.18s |

Both runs scanned 10,019 legacy indexes and 376,346,710 SlateDB records in
37,769 chunks across 256 ranges. R42 recorded 35,556,163,087 scan bytes, versus
35,556,163,101 in r41. Both exited 124 with CLI cancellation 130 and zero swaps.
The one-location partial-count difference in r42 can occur when cancellation
interrupts advancement of the two iterators; it is not a mismatch verdict.

R42 performed 16 fewer merge groups and recorded 23.7% less cumulative scratch
traffic despite reaching substantially more comparison work. This supports
removing the cross-partition merge, not merely relabeling it. It does not
establish a completed-check speedup: neither run finished, their audit times
differ, they were sequential single runs, and their work boundaries differ.
Zero partial mismatch counters and full selected coverage are not correctness
acceptance. Scratch peak remains almost unchanged because earlier catalog and
legacy work still controls it. CPU consumption was effectively unchanged over
the capped run; no total CPU-saving claim is made.

Raw checksums and empty scratch cleanup passed; the daemon remained PID 431717,
epoch 55, read-write with zero transactions/intents and its adopted executable
unchanged. The candidate remains uncommitted and is not installed. Further work
should target catalog reduction and the legacy global merge, or partition both
sides for bounded parallel exact comparison. A complete differential verdict
and matched repeats remain required before runtime acceptance.

Artifacts are under `db.test/phase33-ordered-partitions-2026-09-23` and
`db.test/phase33-production-2026-09-23-stream32-ordered-partitions-full-r42`.

## Full-check tail attribution (r41, 2026-09-23)

After release commit `326450df5`, a diagnostic-only CLI split the broad
`catalog_join` progress label into catalog work, legacy iterator preparation,
SlateDB iterator preparation, and exact location comparison. Additional cleanup
and aggregate-validation labels distinguish later work. A spilled-input test
checks the stage sequence and unchanged comparison output. Focused tests and
affected maintenance/index-command race tests passed.

The full HDD-NFS diagnostic used the adopted daemon without restart, 32 workers
and RPCs, unchanged 96 GiB checker memory and scratch limits, and a ten-minute
cap. No builds or tests overlapped. Stage boundaries, rounded by progress output:

| Stage | Start | Duration before next stage/cap |
| --- | ---: | ---: |
| Legacy scan | 0s | 98s |
| Encryption audit | 98s | 56s |
| SlateDB scan | 154s | 75s |
| SlateDB finalization | 229s | 16s |
| Catalog join | 245s | 183s |
| Legacy location merge preparation | 428s | 69s |
| SlateDB location merge preparation | 497s | 95s |
| Exact location comparison | 592s | 8s, interrupted |

All 10,019 legacy indexes and 376,346,710 SlateDB records were scanned, with
37,769 chunks and 256 completed ranges. Only 7,158,424 locations on each side
were compared before cancellation. Zero partial mismatch counters do not imply
a clean verdict. Exit was 124 (CLI cancellation 130), with 613.26 seconds wall
time including cleanup, 3,908.50 CLI CPU seconds, peak RSS 119,816,020 KiB,
zero swaps, 30 merge groups, and peak scratch 70,240,992,196 bytes.

The result does not support attributing the whole tail to the serial comparison:
catalog work and iterator preparation consumed almost all of the post-scan
window. The longer audit also prevents treating r40/r41 as matched runtime
controls. The next narrow candidate preserves SlateDB's disjoint blob-ID
partitions through ordered iteration instead of merging them into a global
spool. Necessary within-partition merges become lazy comparison work; total
runtime, merge activity and exact results remain the evaluation criteria.

Raw artifact checksums and empty scratch cleanup passed. The daemon remained
PID 431717, epoch 55, read-write with zero transactions/intents. No production
binary was installed. Artifacts are under
`db.test/phase33-location-attribution-2026-09-23` and
`db.test/phase33-production-2026-09-23-stream32-location-attribution-full-r41`.

## Retained-memory spill experiment, 2026-09-23

The parallel legacy consumer and r39 evidence were committed as `35724eb1b`,
without pushing. The next working-tree change preserves sorted memory runs when
a spool reaches its tuple-memory budget. Instead of writing every retained run
to disk, automatic overflow writes one run and reuses its backing array as the
disk-mode buffer. Other runs remain in memory within the same capacity accounting
and are consumed by the existing mixed memory/disk iterator. Explicit full-spill
behavior is unchanged.

Focused tests verify exact set and multiset output, retained-run capacity, and
a scratch limit that cannot accommodate spilling all runs. Existing full-spill
retry coverage now invokes that explicit helper. The maintenance and index-command
suites passed with the race detector, and formatting/editor checks passed.

Full HDD-NFS run `stream32-retained-spill-full-r40` kept 32 workers/RPCs, 96 GiB
checker memory, 96 GiB scratch, and the ten-minute cap. No builds/tests or known
backup workloads overlapped it. The adopted daemon was unchanged and was not
restarted. The isolated Go 1.27.1 profile CLI SHA-256 was
`9c55f5f699762b22c5b996f46fc6bf6828ed38f9b853e6bab59e80d52dd3d43d`.

All 10,019 legacy indexes loaded by about 97 seconds, with 29,497,229,840 bytes
of scratch, compared with 181 seconds and 83,631,727,424 bytes in r39. Auditing
finished at about 111 seconds. All 256 blob ranges completed by 185 seconds:
376,346,710 records, 35,556,163,076 bytes, and 37,769 chunks. Finalization reached
`catalog_join` at 203 seconds. Scratch peak was 70,418,506,212 bytes (about
65.6 GiB), and 30 merge passes were recorded. This run progressed beyond the
previous scratch-exhaustion boundary without increasing either configured limit.

The ten-minute cap interrupted work still labeled `catalog_join`; this label
also covers subsequent location comparison. Partial location counters had reached
87,054,373 legacy and 87,054,372 SlateDB locations, so the stage label alone must
not be used to attribute the entire wait to catalog reduction. The wrapper exited
124 and the CLI reported cancellation code 130. Total time including cleanup was
611.78 seconds, CLI CPU was 4,000.78 seconds, peak RSS was 122,151,980 KiB (about
116.5 GiB), and no swaps occurred. The checker tuple budget is not an RSS cap.

There is no final differential verdict or end-to-end acceptance. In particular,
the partial result's `coverage.complete: true` describes the selected full-check
coverage, not successful execution after cancellation. Neither incomplete location
counts nor mismatch counters establish correctness. These sequential single runs
are not matched repeated controls, and their different completion boundaries do
not establish total CPU savings. The next diagnostic should distinguish catalog
reduction, location merge, and exact comparison before choosing another change.

Scratch cleanup and raw artifact checksums passed. PID 431717 remained read-write
at epoch 55 with zero transactions and write intents, and its executable still
matched the adopted binary. No installed binary, backend, or resource limit was
changed. The retained-memory change remains uncommitted and native RADOS acceptance
remains open.

Artifacts are under
`/volume2/NASDA2/rustic/db.test/phase33-retained-spill-2026-09-23/`
and the raw run directory
`/volume2/NASDA2/rustic/db.test/phase33-production-2026-09-23-stream32-retained-spill-full-r40/`.

## Parallel legacy consumer experiment, 2026-09-23

The diagnostic attribution changes were committed as `41207ec50` with a detailed
implementation and evidence message, without pushing. The subsequent working-tree
experiment removes the serialized `ForAllIndexesWorkers` callback from the full
check's legacy loader. Up to 32 workers own private location and pack-multiset
spools, with the existing total spool budgets divided among workers. Workers are
also limited by the number of tuples those budgets can hold. Final buffers are
finished concurrently, then adopted using the parent context rather than the
completed worker group's canceled context. Shared legacy-index APIs are unchanged.

Local validation passed the maintenance and index-command package suites, plus
focused race tests. Serial-reference tests cover worker requests 0/1/4/32,
one-tuple, eight-tuple, and memory-only budgets, optional pack contributions,
duplicate locations, and preserved pack multiplicity. Failure tests cover malformed
input, pre-cancellation, scratch exhaustion, and private spill cleanup. These are
correctness fixtures, not representative-storage performance measurements.

One full HDD-NFS diagnostic, `stream32-legacy-parallel-full-r39`, used 32 workers
and RPCs, 96 GiB checker memory, 96 GiB scratch, and the ten-minute cap. No builds
or tests overlapped the measurement and no known backup workload was observed.
The adopted daemon was not restarted or replaced. The separate CLI was built
with Go 1.27.1 and `selfupdate,disable_grpc_modules,profile`, with SHA-256
`fff9713f3172f6b47fdfc6b0d532245c3659399a3a298a2b8838fcfe24295a75`.

The legacy scan loaded all 10,019 indexes and reached encryption auditing at
about 181 seconds, with 83,631,727,424 bytes of scratch. Auditing completed at
about 196 seconds. The SlateDB scan then exhausted the unchanged scratch cap:
it needed another 76,895,512 bytes with 103,009,415,936 of 103,079,215,104 bytes
already used. The CLI exited 1 after 266.29 seconds including cleanup, before
any merge pass or final differential verdict. Partial scan counters were
250,485,342 records, 25,127 chunks, and 155 completed ranges. Zero final location
or mismatch counters in this failed result are not evidence of equivalence.

CLI CPU was 3,438.25 seconds and peak RSS was 86,211,860 KiB, with no swaps.
Host interval samples averaged 32.77% idle and 10.93% I/O wait. The earlier r34
serial run had not finished legacy scanning at the ten-minute cap, but it is not
a matched control. This establishes progress beyond the previous bottleneck,
not an end-to-end speedup ratio, CPU reduction, or full-check acceptance.

Scratch was empty after failure. The same daemon PID 431717 remained read-write
at epoch 55 with zero transactions and write intents. No installed CLI, daemon,
backend, or resource limit was changed. The next gate needs an explicitly agreed
scratch budget or bounded spill reduction, followed by a completed full check.
Native RADOS validation remains blocked by the previously recorded prerequisites.

Raw artifacts and their verified manifest are under
`/volume2/NASDA2/rustic/db.test/phase33-production-2026-09-23-stream32-legacy-parallel-full-r39/`.
The source patch, candidate binary, build and test logs, and harness are under
`/volume2/NASDA2/rustic/db.test/phase33-legacy-parallel-2026-09-23/`.

## Local streaming follow-up, 2026-09-22

The working-tree follow-up implements a negotiated `ScanStream` RPC. After
explicit operator approval it was deployed to `vaulticdb-rustic.service` using
the configured clean-demotion stop hook. Preflight found zero active write
intents and transactions; restart advanced writer epoch 37 to 38 and restored
the read-write role. The earlier measurements below still describe the unary
implementation; they are not evidence of a streaming speedup.

Matched profile binaries were built with `make profile BIN_DIR=bin/phase33-stream`.
The deployed CLI SHA-256 is
`0a73f3c753843eb67ea7d966ba33a756ee14ecda80371c14c3f0122ebcdf8267`;
the daemon SHA-256 is
`211d77ca61951ec0d709faa9354630308dd44045b1b69b5bfd0a2d9627a19fbe`.
Previous binaries are retained under `bin/phase33-stream/rollback/`.
The ten-minute diagnostic artifacts are under
`/volume2/NASDA2/rustic/db.test/phase33-production-2026-09-22-stream32-r1/`,
including source revision/diff and binary hashes. It uses 32 workers/RPCs,
96 GiB checker memory, 96 GiB scratch on the same NFS parent, reduced SlateDB-only
coverage, encryption auditing and crawl-debt details. No production caches were
dropped. The daemon restart resets its process-local caches, so this is not an
identical warm-cache control against the earlier runs.

The new path retains one pinned-transaction iterator per range, including the
lookahead record at item and byte boundaries. The daemon admits at most 32
streams, each with one queued response and bounded chunk construction. Responses
remain below 16 MiB including telemetry, with one retained lookahead record.
These are application-buffer bounds, not a combined RSS guarantee: SlateDB
iterator/cache allocations and transport buffers remain additional costs.
Receiver cancellation drops the iterator; delivery waits are bounded by the
transaction idle timeout and a range lasts at most one hour or the shorter
caller deadline. There is no reconnect/resume against a new transaction.

The Go client negotiates the capability, checks ordering, prefix coverage,
response limits and explicit completion, and cancels on consumer failure.
Older daemons retain the pinned unary path. Checker wrappers hold one RPC permit
through the whole range and exclude tuple consumption from database wait spans.
Session identity is validated after each completed range and before success.
Expired ordinary transaction reads now fail instead of refreshing an already
expired lease. The initial streaming patch required enough idle time for
non-database stages; the subsequent lease follow-up below adds renewal.

Progress and result resources expose records, protobuf bytes, chunks,
started/completed ranges, iterator setup/service nanoseconds, receive wait and
consumer time. Final results retain at most 256 first-byte partition summaries.
Receive wait includes server work and delivery; it is not pure network latency.
Existing backend metrics remain separate: per-scan table/object read attribution
and active continuation age are not implemented by this follow-up.

Local validation:

```text
cargo test --manifest-path vaulticdb/Cargo.toml --all-targets
cargo fmt --manifest-path vaulticdb/Cargo.toml --check
cargo build --manifest-path vaulticdb/Cargo.toml --bin vaulticdb
go test ./internal/index/daemon -count=1
go test ./internal/index/maintenance ./cmd/vaultic/indexcmd
go test -race ./internal/index/daemon ./internal/index/maintenance ./cmd/vaultic/indexcmd \
  -run 'TestReadSession|TestScanStream|TestScanRange|TestLoadSlateDBLocations|TestCheck|TestIndexCheck' -count=1
```

The native all-targets run passes 307 tests. Owner suites and focused race checks
pass. Fixtures cover persistent item/byte boundaries, snapshot parity, one
iterator setup per range, stream capacity, expiry/rollback, cancellation cleanup,
malformed/truncated responses, legacy fallback, and wrapper admission cleanup.
The CI Clippy command remains blocked by unchanged `double_parens`,
`obfuscated_if_else`, `collapsible_if`, `unnecessary_lazy_evaluations`, and RADOS
`dead_code` warnings. No unrelated lint fixes are included.

### Streaming diagnostic outcome

The run was interrupted before its configured ten-minute cap. It exited 130
with `context canceled`, not timeout status 124, after 8m37.56s including cleanup.
The captured artifacts alone did not establish the source of cancellation. A
subsequent forced-spill regression reproduced self-cancellation during spool
adoption after successful `errgroup.Wait()`; see the follow-up below. The last
periodic progress was at 8m25s; no completed-check verdict was produced.

Encryption audit completed at about 1m04s. By the 8m20s sample, all 256 ranges
had completed, delivering 376,346,710 blob records in 37,769 chunks and
35,556,199,869 protobuf bytes. These are blob-key records, not the imported
location/multiplicity count of 379,934,385; their different cardinalities alone
do not imply missing data. The checker still reported `slatedb_scan` after range
completion: spool adoption/finalization precedes `catalog_join`, which was not
reached. The entire scan stage therefore remains incomplete.

| Metric | Interrupted stream32 r1 |
|---|---:|
| CLI user / system CPU | 1,674.24 s / 185.91 s |
| CLI peak RSS | 99,071,456 KiB (94.5 GiB) |
| Major faults / swap | 0 / 0 |
| Encrypted scratch peak | 59,103,515,330 B (55.0 GiB) |
| Daemon user + system CPU | 6,536.77 s |
| Daemon logical / physical reads | 2,625,923,374,777 B / 9,428,381,696 B |
| Main object-store GET attempts | 9,699,631 |
| Main object-store GET-body bytes | 43,026,979,884 B |
| Host NFSv3 operations / getattr | 11,931,784 / 9,933,334 |
| Host NFSv3 reads / writes | 146,716 / 860,738 |
| Iterator setup / service | 12.234 s / 12,507.482 worker-s |
| Client receive / consume | 11,539.735 s / 1,993.562 s |

Worker/service/receive times overlap and must not be added into wall time.
Backend body bytes and process logical reads measure different boundaries.
NFS counters include unrelated host activity. The old buffered32 run accumulated
2,490,400,225,853 logical read bytes and 7,870,426 host `getattr` calls over its
longer timeout run, so this experiment does not establish reduced read
amplification or a matched end-to-end speedup. Stream delivery completion is
new progress, but it is not complete scan-stage or full-check acceptance.

After cancellation, the service remained read-write at epoch 38 with zero active
transactions/write intents; the diagnostic process exited and the configured
scratch parent had no remaining session directories. The artifact manifest
verified successfully. Rollback binaries remain available; the streaming
candidate remains deployed.

### Finalization and lease follow-up

Commit `e06a8ffd6` records the streaming implementation and r1 results. The next
patch reproduces the finalization failure using three records in one partition
with a two-record memory buffer: the remaining spilled record must be flushed
during adoption. `errgroup.Wait()` cancels its context even when every worker
succeeds. Partition spools retained that context, so encrypted `writeRun` failed
with `context canceled`. The fixture fails before the fix and passes afterward.

After workers join successfully, adoption now uses the parent check context.
An explicit cancellation at the adoption boundary still fails, proving the fix
does not detach from caller cancellation. Progress reports `slatedb_finalize`
separately from range delivery and `catalog_join`.

Read sessions now renew by validating the pinned sequence and generation on a
bounded background request. New daemons advertise transaction idle milliseconds
in `BeginResponse`; the interval is one-third of that timeout, capped at 30s,
with a request timeout at most 10s and no longer than the interval. Older daemons
use a 1s interval and a 10s request timeout; unknown custom lease settings are not
guaranteed to survive, and renewal failure fails incomplete. No expired lease
is revived and no replacement transaction is opened. The CLI runs the checker
with the failure-aware session context; close cancels and joins the renewal
worker before rollback. Virtual-time tests cover idle work, bounded stalled
renewals, sticky errors, and cancellation cleanup. Real daemon tests cover timeout
advertisement and session close.

Renewal runs at most one control request at a time outside the checker's scan
RPC semaphore, so the configured 32-RPC scan budget is not a strict total RPC
cap: up to one additional renewal request can be active. Reserving this control
capacity prevents long-lived range streams from starving lease maintenance.

The affected Go owner suites, full owner race suites and 62 native storage tests
pass. A later parallel native all-targets run failed two existing tests because
`clean_incomplete_memory_wal_rebuild_does_not_handoff` consumed the global
`InventoryWal` failpoint intended for
`dedicated_wal_inventory_failure_precedes_cache_startup`. A focused serial rerun
of the daemon target passed all 190 tests, including both failures:

```text
cargo test --manifest-path vaulticdb/Cargo.toml --bin vaulticdb -- --test-threads=1
```

The broader serial retry passed all 102 library tests before being deliberately
stopped during unrelated broker tests; it is not a completed all-targets pass.
No unrelated test changes are made.
The native timeout field was validated locally but does not require another
production restart: diagnostic r2 uses the new profile CLI against the existing
epoch-38 streaming daemon via the legacy timeout-advertisement fallback.

The repeated ten-minute diagnostic is captured under
`/volume2/NASDA2/rustic/db.test/phase33-production-2026-09-22-stream32-finalize-r2/`.
It preserves 32 workers/RPCs, 96 GiB memory/scratch and the same scratch parent,
with no cache drops, daemon replacement or restart. The CLI SHA-256 is
`8c86b3b768cfd8f30e234c829334b7410cda82f32e52ae6a8733d9cdc8be22b7`;
the daemon hash is unchanged from r1. Local regression workloads overlapped this
run, so timing is diagnostic rather than an isolated performance comparison.

R2 completed the audit at 1m35s and entered `slatedb_finalize` at 9m03s after
delivering the same 376,346,710 records in 37,769 chunks across all 256 ranges.
It remained in finalization until the actual ten-minute timeout (wrapper exit
124), then returned the expected canceled outcome and cleaned up. Wall time
including cleanup was 10m19.47s. This confirms the premature self-cancellation
is fixed, not that the complete scan stage or check finishes within ten minutes.

CLI user/system CPU was 1,662.47/210.22 seconds; peak RSS was 102,249,352 KiB
(97.5 GiB), with no swap. Scratch grew from 59,055,812,608 bytes at finalization
entry to 66,767,726,000 bytes (62.2 GiB) at cancellation, with zero merge passes.
Daemon CPU totaled 6,515.28 seconds. Iterator setup was 11.704 seconds;
service/receive/consume worker times were 12,787.753/11,960.122/1,997.716 seconds.
These overlapping times must not be summed into wall time.

The artifact manifest verified. The writer stayed read-write at epoch 38 with
zero active transactions/intents, no diagnostic process remained, and scratch
cleanup left no session directories. Only the diagnostic CLI changed; production
service binaries remain the r1 deployment.

### Bounded parallel finalization

The follow-up to `d4d5d223e` flushes pending partition buffers in a separate
`errgroup` capped by `--check-workers`, after all scan workers join and before
serial ownership transfer. Each worker finishes one partition's location and
pack buffers sequentially, so at most one run writer per worker is active.
Existing partition buffers, shared scratch reservations and run-name allocation
are reused; no additional tuple buffers or per-partition memory allowances are
introduced. Each active writer retains the existing 1 MiB output buffer and
encryption allocations, outside the tuple-memory budget.

Workers use the finalization group's cancellation context. All workers join on
success or failure before cleanup; successful ownership transfer restores the
parent context and checks caller cancellation. The `slatedb_finalize` stage
remains separate, allowing measurement of this tail independently of delivery.

Focused virtual-time regressions cover one and four simultaneous finalizers,
exact sorted location/pack results, memory bounds, cancellation during active
writes, scratch exhaustion, joined workers, released reservations and scratch
directory cleanup. The existing spill and scan regressions pass, as do:

```text
go test -race ./internal/index/maintenance ./cmd/vaultic/indexcmd -count=1
make vaultic BIN_DIR=bin/phase33-parallel-finalize VAULTIC_BUILD_TAGS=profile
```

Diagnostic r3 uses that separate CLI against the unchanged epoch-38 daemon,
with the same 32-worker/RPC, 96 GiB memory/scratch and ten-minute limits.
Tests and builds completed before measurement; no local test/build workloads
overlap this run, and there is no cache drop, daemon replacement or restart.
Artifacts are under
`/volume2/NASDA2/rustic/db.test/phase33-production-2026-09-22-stream32-parallel-finalize-r3/`.
The CLI SHA-256 is
`711d95b7c08bc7192c6725596f61b6d175c70740fcefb632b49efbb1edb7e248`;
the daemon hash is unchanged from r1. Starting host load averages were
2.00/3.64/8.91. Cache state still differs from earlier attempts; a single bounded
repeat is not a matched end-to-end speedup measurement.

R3 entered `slatedb_scan` at the 1m55s sample and remained there until the
ten-minute timeout (wrapper exit 124). It delivered 375,902,743 records in
37,720 chunks, totaling 35,514,250,080 protobuf bytes; 250 of 256 ranges completed.
Neither `slatedb_finalize` nor `catalog_join` was reached, so production behavior
and performance of the new finalizer pool remain unverified. The slower scan
cannot be attributed to a finalization path that did not execute.

| Metric | Capped parallel-finalize r3 |
|---|---:|
| Wall time including cleanup | 10m19.20s |
| CLI user / system CPU | 1,440.90 s / 174.49 s |
| CLI peak RSS | 101,242,676 KiB (96.6 GiB) |
| Swap | 0 |
| Encrypted scratch peak | 59,055,812,608 B (55.0 GiB) |
| Merge passes | 0 |
| Daemon CPU | 6,433.03 s |
| Iterator setup / service | 13.301 s / 14,120.982 worker-s |
| Client receive / consume | 13,409.859 s / 1,752.187 s |

The artifact manifest verified. Scratch cleanup left no session directories,
only the daemon process remained, and the writer stayed read-write at epoch 38
with zero active transactions and write intents. Production service binaries
were not replaced. Local exact-result, concurrency and race tests pass; this
production run is incomplete evidence, not finalization or full-check acceptance.

Next: obtain stage-scoped finalization timing without spending most of each
feedback window rescanning the database, and investigate scan/audit variability.
Then assess finalization completion, profile catalog joins and later validators,
and run full differential coverage. The ten-minute diagnostic limit remains in
force; full differential success and the representative acceptance matrix remain
open.

### Attribution Gate

The optimization objective is minimum complete-check runtime and maximum useful
throughput, not minimum CPU or RAM consumption. Additional resource use is
acceptable when it improves that objective within explicit safety limits.
Further tuning requires an attributable bottleneck hypothesis, a distinguishing
measurement and a matched correctness/throughput comparison.

R1-r3 captured aggregate resource use but did not capture production CPU stacks
or persist checker dependency-wait snapshots. Those runs do not establish which
functions consumed CPU or distinguish storage latency from scheduling and
backpressure inside iterator service. Low host I/O-wait is not evidence that
application storage waits are absent. Service, receive and consume timers are
overlapping elapsed worker times, not CPU-time partitions.

The attribution follow-up adds opt-in `index check --monitor-export-jsonl` using
the existing bounded asynchronous exporter: five-second checker and daemon/cache
snapshots, a four-snapshot queue, two-second collection/drain timeouts and
explicit dropped/failure counters. Initial spill runs expose separate sort,
encode/write, buffered flush and `fsync` elapsed histograms. Measurements occur
at run boundaries, not per record. Encode/write includes encryption, allocation,
buffered writes and any full-buffer kernel writes; CPU profiles distinguish
those costs. These stage timers currently cover initial spill runs, not the
later merge writer. `slatedb_finalize` maps to the finalization telemetry phase.

The r4 attribution diagnostic captures a Go CPU profile, a three-second scan
execution trace, five-second goroutine and Linux thread/wait-channel samples,
and timestamped daemon `perf` CPU call stacks with context-switch records.
The unchanged deployed daemon has symbols and frame pointers. Profiling is
local, artifacts are protected, no service restart is required, and the check
retains the ten-minute cap. Profiling overhead means this is an attribution
run, not a clean performance comparison. Tests/builds finish before measurement.

Collection uses the profile-build CLI's existing `--cpu-profile DIR` and
`--listen-profile 127.0.0.1:PORT`, plus the checker's `--monitor-export-jsonl FILE`.
The trace comes from `/debug/pprof/trace?seconds=3`, goroutine stacks from
`/debug/pprof/goroutine?debug=1`, and daemon sampling uses
`perf record -e cpu-clock -F 49 --call-graph fp --switch-events --timestamp
--clockid mono -p PID`. All collectors stop before artifact checksums are made.
Context-switch capture can produce multi-GiB artifacts; retain it for attribution
runs, not ordinary throughput comparisons. The monitor performs serial status
requests outside scan admission, adding at most one concurrent control request
beside the existing single renewal request. Export drops/failures and incomplete
profiles must be reported, not silently treated as zero waits.

For each experiment, report stage wall time and records/s first, then process
CPU-seconds, peak RSS, scratch and backend bytes/record. Attribute CPU using flat
and cumulative stack profiles; attribute waits using their owner, downstream
resource, contention count and elapsed distribution. Do not add nested profile
percentages or overlapping worker spans, and do not count intentionally idle
maintenance threads as bottlenecks. Use matched unprofiled repeats to accept a
performance change after an attribution run identifies the controlling work.

The raw r4 artifacts are under
`/volume2/NASDA2/rustic/db.test/phase33-production-2026-09-22-stream32-attribution-r4/`.
The measured CLI SHA-256 is
`ee8b0cf16d03f36317afb753fa2ddd69f105d2ef83b9083c3d3e6f300e150cf6`.
The audit ended at 52s; timeout 124 stopped `slatedb_scan` with 376,304,659 records,
37,762 chunks and 252/256 ranges complete. Finalization was not reached. Check
wall time including cleanup was 10m18.67s, CLI CPU was 1,951.47s user plus
191.90s system, peak RSS was 103,139,192 KiB (98.4 GiB), scratch was 55.0 GiB,
and swap was zero. Daemon status deltas include the subsequent perf drain:
6,848.29 CPU-seconds over 702.067s, not just checker wall time.

The raw manifest verified; the nested CLI CPU profile and derived reports have
supplemental checksums under `analysis/`. The writer stayed read-write at epoch
38 with zero transactions/intents; only the daemon remained and scratch was
empty. The service binaries are unchanged.

#### Measured Wait Owners

- 2,008 of 2,533 sampled scan-worker observations (79.3%) were in gRPC receive;
  369 were in spill writes, 34 in `fsync`, 34 in sorting, and 88 elsewhere.
  These are sampled worker states, not exact wall-time shares.
- The three-second trace at 20:23:34-37 UTC shows 77.02 worker-seconds blocked in
  scan receive, about 5.02ms of scan-worker scheduling delay, and 2.62s in the
  transport reader's network poll. This supports waiting for responses rather
  than client CPU scheduling as the controlling delay in that window.
- RPC admission had zero contentions and 442us total across 257 admissions.
  Recorded daemon transaction-map/slot waits totaled 386us/0.528s respectively.
  These metrics do not measure every iterator-internal lock.
- Database GET request service totaled 13,946.089 worker-seconds over 9,699,251
  calls. It includes local backend work and async scheduling, not pure NFS wait.
  The separately recorded body-stream time was 24.750s; these boundaries must
  not be interpreted as a complete I/O-time partition.
- Across 1,024 initial spill runs, sorting totaled 166.536 worker-seconds,
  encode/write 1,597.465s, buffered flush 0.212s and `fsync` 232.419s. These
  overlapping worker spans are not additive to overall runtime.

#### Measured CPU Owners

The Go CPU profile has 2,124.61 sampled CPU-seconds. `ActionGuard.Processed`
accounts for 53.57% cumulatively beneath per-record scratch accounting;
atomic compare-and-swap alone is 25.93% flat. This is a demonstrated telemetry
contention cost, not encryption or useful record validation. `writeRun` is
70.25% cumulative and includes that accounting cost; do not add those shares.
Allocation (`mallocgc`) is 9.46% cumulative and AES-GCM encryption 3.25% flat.

The daemon profile contains 331,192 CPU samples and reports zero lost samples.
It reports 75 out-of-order context-switch events, so no exact off-CPU duration
is inferred. Flat CPU includes 17.48% buffer zeroing (17.44% traced to
`object_store::local::read_range`), 14.55% AES-GCM (callers resolve to
`EncryptedObjectStore::decrypt_chunks_sync`), 6.90% kernel copying, and 4.13%
user-space copying. These establish allocation/copy/decryption costs on the
read path; they do not prove that increasing workers or cache alone will help.

#### Post-Run Corrections

Scratch accounting now sits below the existing 1 MiB buffer, so shared counters
are updated per underlying write rather than twice per tuple. The encrypted
format and successful byte totals are unchanged; canceled bytes now count only
bytes accepted by the underlying writer, not unflushed buffer contents. Exact
byte/short-write, cancellation, corruption, spill and cleanup tests pass.

The accounting-only benchmark, 32-way concurrency and three one-second repeats,
measured 794,980/810,203/817,690 ns per fixed batch at the old accounting boundary
versus 6,525/6,450/6,453 ns buffered. This excludes encryption and disk I/O and is
not an end-to-end speedup. A follow-up local CPU profile is dominated by buffer
writes/copies, with shared telemetry updates absent from the leading costs.
Production runtime improvement still needs a matched capped repeat.

R4 preserved 120 valid monitor snapshots; one additional snapshot was rejected
because daemon histogram count and buckets were sampled inconsistently under
concurrent updates. The adapter now marks only that histogram unavailable rather
than dropping the entire snapshot or fabricating coherent values. Exporter
queue drops/failures were zero; the rejected collection is a separate gap.

Future Go profiles carry `check_stage` labels inherited by stage workers and
restored after the check. These labels, histogram handling and batched accounting
are post-r4 changes, not part of its measured binary. Final race suites pass:

```text
go test -race ./internal/index/maintenance ./internal/telemetry ./cmd/vaultic/indexcmd -count=1
```

Next hypotheses: confirm the accounting correction in a matched production
repeat, then investigate unnecessary local read-range initialization/copying and
repeated encrypted-range work. Use available CPU/RAM when the measured working
set or concurrency bottleneck justifies it; do not trade away verification or
authenticated encryption for throughput.

Remaining attribution boundaries must stay explicit: Linux thread sleep is not
equivalent to a parked Rust async task; CPU samples cannot quantify async wait
duration; the existing server iterator timer excludes output-channel reservation
wait. A daemon-side async wait breakdown is still needed if the captured CPU,
cache and client-wait evidence cannot discriminate the next hypothesis.

### Accounting Comparison Follow-Up

The available storage choices are native RADOS and HDD-backed NFS, not local
disk. The current checker scratch writer requires filesystem operations; RADOS
is not a drop-in value for `--check-temp-dir`. Prioritize avoiding unnecessary
spill and repeated reads using RAM before considering a new scratch backend.
Do not assume historical SSD test targets remain available.

Commit `80ab5b4e6` preserves bounded parallel finalization, attribution and the
post-r4 accounting correction. Its separate profile-tag CLI has SHA-256
`f5d89abe485b9bbf0fbc9935399cb0a7afa487a67f2399d520664eb6146eddde`.
The retained pre-fix r4 CLI is the comparison control. Both runs keep the daemon,
96 GiB checker budget, 96 GiB NFS scratch limit, 32 workers/RPCs, encryption and
SlateDB-only coverage unchanged. Each is capped at ten minutes with TERM and a
45-second kill grace. Five-second JSONL, process and host samples remain enabled;
CPU profiling, execution traces and perf context-switch capture are disabled.
No cache drops, daemon restart, builds or tests overlap the runs. Sequential
cache warming and backend variability remain confounders; a single pair cannot
establish a repeatable end-to-end speedup. The candidate harness mirrors existing
progress lines through `tee`; the control redirects them only to its log. Both
retain exact harness copies. The workspace revision in the control artifacts is
the harness workspace, not the retained pre-fix binary's source revision; use its
binary hash and the r4 source patch for measured-code provenance.

#### R5/R6 Results

Artifacts are under the same `db.test` root, with names
`phase33-production-2026-09-22-stream32-accounting-control-r5` and
`phase33-production-2026-09-22-stream32-accounting-batched-r6`.
Both manifests verified. Each hit timeout 124, returned incomplete SlateDB-only
coverage, cleaned scratch, and left the unchanged writer healthy at epoch 38
with zero transactions/intents. Neither run is full-check acceptance.

| Measurement | Pre-fix r5 | Batched r6 |
| --- | ---: | ---: |
| Encryption audit | 51s | 51s |
| Location scan (stage duration) | 547s | 454s |
| Location records / completed ranges | 376,346,710 / 256 | 376,346,710 / 256 |
| Finalization start (elapsed) | 9m58s | 8m25s |
| Catalog join start (elapsed) | Not reached | 8m57s |
| CLI user + system CPU seconds | 1,709.48 | 1,362.34 |
| Wall time including cleanup | 10m21.45s | 10m23.49s |
| Peak RSS (KiB) | 101,765,404 | 115,474,960 |
| Scratch peak (bytes) | 60,589,131,042 | 85,342,828,262 |
| Merge passes | 0 | 4 |
| Daemon logical read bytes | 2,625,980,781,200 | 2,625,996,773,108 |
| Daemon CPU seconds | 6,405.55 | 6,451.37 |
| Monitor snapshots | 121 | 121 |
| Swaps | 0 | 0 |

The observed scan stage is 17.0% shorter (20.5% greater records/s), and total CLI
CPU is 20.3% lower despite the candidate reaching more work. The candidate
finishes bounded parallel finalization in 32s. Greater RSS/scratch and four
merges are associated with reaching later stages, not equal-work resource
regressions. Complete-check runtime remains unknown. Both binaries already
contain parallel finalization; the pair compares the accounting correction plus
the post-r4 histogram handling and profile-label changes, not an isolated
single-instruction A/B. Retain the result as provisional pending repetitions.

GET counts remain approximately 9.7 million and daemon logical reads remain
2.63 TB. Accounting reduces client cost without fixing read amplification.
The next backend candidate therefore targets fetch granularity rather than
increasing RPC admission or moving scratch to an unavailable local disk.

#### Next Read-Path Hypothesis

The retained streaming iterator calls `scan_prefix_transaction`, which uses
SlateDB's default `ScanOptions`: `read_ahead_bytes=1` (one block per fetch),
`max_fetch_tasks=1`, and `cache_blocks=false`. The encrypted store rounds each
requested plaintext range to complete authenticated chunks (256 KiB by default),
decrypts them, then returns the requested slice. Adjacent small-block requests
can therefore repeatedly fetch and decrypt overlapping chunks. The local-file
object-store implementation also zero-initializes the expanded read buffer.

R4 recorded 9,699,251 database GETs and 43,073,470,791 plaintext GET-body bytes;
daemon `/proc` logical read bytes increased by 2,625,792,550,135. The roughly
61-fold aggregate ratio supports this hypothesis but is not an exact per-read
amplification measurement: the counters have different boundaries, the process
window includes perf drain, and logical reads are not physical NFS traffic.

The next narrow candidate implements 1 MiB read-ahead for retained streaming iterators
only, retaining one fetch task per SST and the 32-stream limit. SlateDB bounds
fetch block ranges to the iterator range; this does not remove authentication,
change snapshots, or require new plaintext disk caching. Memory scales with
active SST iterators, not just stream count, so measure daemon RSS rather than
claiming a fixed 32 MiB total. Verify exact ordered keys/values, exclusive cursor
and prefix boundaries on persisted SSTs, fewer backend GETs, and existing stream
expiry/cancellation/cleanup behavior before any production deployment.

Local implementation and validation completed before the approved deployment
recorded below. Unary scans retain default options. The persisted-SST regression
uses disabled block caching and checks full keys/values, neighboring prefixes,
exact and between-key exclusive cursors, empty results and a pinned snapshot
excluding later writes. Streaming performs over eight times fewer GETs than
default one-block fetching in this fixture. This is a backend-call regression,
not a production runtime or encrypted-byte reduction claim. Existing stream
snapshot/expiry/admission/cleanup tests pass, as do all 63 storage tests:

```text
cargo test --manifest-path vaulticdb/Cargo.toml --bin vaulticdb storage::tests:: -- --test-threads=1
```

Additional RAM remains a separate experiment: the checker currently divides its
budget into four spool shares even for SlateDB-only coverage. Size it from tuple
footprints and peak later-stage memory rather than assuming the entire configured
budget is available to each ordering.

#### R7 Read-Ahead Deployment And Result

With explicit deployment/restart approval, the static frame-pointer release build
passed and only the daemon executable was replaced. The previous daemon remains
at `bin/phase33-read-ahead/rollback/vaulticdb`. The deployed SHA-256 is
`2963b4456fe2cd7f1738f19635745060f5f41df307c496a10e54a917f20a4305`;
PID 100168 reopened read-write at epoch 39. Configuration, service CLI and storage
were unchanged. Deployment provenance is under
`db.test/phase33-read-ahead-deployment-2026-09-22`; diagnostic artifacts are under
`db.test/phase33-production-2026-09-22-stream32-read-ahead-r7`.

The same r6 accounting CLI and resource limits produced:

| Measurement | r6 default reads | r7 1 MiB read-ahead |
| --- | ---: | ---: |
| Encryption audit | 51s | 49s |
| Location scan duration | 454s | 117s |
| Location records / completed ranges | 376,346,710 / 256 | 376,346,710 / 256 |
| Finalization duration | 32s | 30s |
| Catalog join start (elapsed) | 8m57s | 3m16s |
| Database GET calls (monitor delta) | 9,699,968 | 41,784 |
| Plaintext GET-body bytes | 43,094,546,875 | 42,809,360,880 |
| Daemon logical read bytes | 2,625,996,773,108 | 93,741,772,175 |
| Daemon CPU seconds | 6,451.37 | 1,580.19 |
| CLI CPU seconds | 1,362.34 | 1,842.18 |
| CLI peak RSS (KiB) | 115,474,960 | 115,470,136 |
| Scratch peak (bytes) | 85,342,828,262 | 85,343,106,436 |
| Merge count | 4 | 21 |

Scan duration fell 74.2% (3.88 times throughput), while logical read bytes fell
96.4%. The aggregate logical/plaintext read ratio is about 2.19 rather than 61;
it is still not an exact scan-only or physical NFS amplification measure. Daemon
high-water RSS since restart was 1,315,328 KiB, ending at 528,488 KiB. The restart
reset daemon caches, and r7 performs more later-stage work, so this is strong
mechanism evidence but not a repeated matched complete-check speedup result.

R7 hit timeout 124 in `catalog_join`, with wall time 10m06.76s including cleanup,
zero swaps and 121 monitor snapshots. Both manifests verified; scratch is empty,
only the daemon remains, and the writer is healthy with zero transactions/intents.
Coverage is still incomplete SlateDB-only, not a full differential clean verdict.

The user's observation of sustained approximately 100% CLI CPU is consistent
with the newly exposed serial merge path: `checkPackCatalog` constructs a spool
iterator, whose `seal()` synchronously merges each fan-in group in sequence.
Each `mergeRuns()` executes record decryption, heap selection and encryption in
one goroutine. R7 records 21 merge calls during its longer catalog-stage window;
there is no r7 CPU profile to assign exact function percentages. The progress
stage includes preparatory merging, not just the final catalog comparison.

Next target: bounded parallel independent merge groups, initially 2-4 workers,
with explicit output-space admission, joined cancellation/cleanup and unchanged
ordered results. Each 32-input group already needs about 32 MiB of input buffers
plus its output buffer, and retains inputs until output publication. With about
79.5 GiB scratch occupied under a 96 GiB limit, launching all groups concurrently
can exhaust scratch. Measure group CPU/waits and HDD-NFS throughput before
increasing concurrency. The eventual final ordered merge/pack aggregation is
also serial and may need a separately validated partitioned design later.

### Bounded Parallel Merge Diagnostic (r8)

The checker now admits up to four independent scratch merge groups per wave,
bounded by the spool's configured buffer budget and shared scratch headroom.
Admission reserves the worst-case output size before starting a group; successful
publication releases unused reservation and removes inputs only after workers
join. Failed waves discard outputs while retaining input ownership for cleanup.
This conservative reservation can reject a merge whose deduplicated output would
fit but whose maximum output would not. The buffer bound is not a total RSS bound.
Other spool callers retain serial merging by default.

The new `check_scratch_merge_dependency_*` family records group outcomes and
elapsed service time, including iterator setup and output flush/sync. It does not
measure CPU time or separate admission, filesystem and crypto waits. Reservation
accounting replaces per-record reservation locking in merge output writers.

Deterministic virtual-time tests cover four simultaneous outputs, scratch- and
buffer-limited single-worker admission, deduplication and multiset ordering,
cancellation, corrupt input, joined workers and exact reservation cleanup.
The maintenance, telemetry and index-command race suites pass. An initial test
run inherited deployment `umask 077`, causing permission-fixture failures; all
three suites passed with `umask 022`. Editor diagnostics and `git diff --check`
also pass.

R8 used the unchanged r7 daemon, the same NFS paths, 96 GiB memory/scratch limits,
32 scan workers/RPCs and ten-minute cap. No build or test overlapped measurement.
The separate CLI at `bin/phase33-parallel-merge/linux-amd64/vaultic` has SHA-256
`8a04b4dde3ec3fd4331086f311932c7321d65b907d0eab4ca9ddcd45666e3fec`;
service binaries were not replaced. Artifacts are under
`db.test/phase33-production-2026-09-22-stream32-parallel-merge-r8`.

| Measurement | r7 serial groups | r8 bounded parallel groups |
| --- | ---: | ---: |
| Encryption audit | 49s | 48s |
| Location scan | 117s | 115s |
| Finalization | 30s | 28s |
| Catalog stage starts | 3m16s | 3m11s |
| Merge calls started by cap | 21 | 24 |
| CLI CPU seconds | 1,842.18 | 2,183.94 |
| CLI peak RSS, KiB | 115,470,136 | 115,318,008 |
| Scratch accounting peak, bytes | 85,343,106,436 | 90,567,039,078 |

All 24 r8 merge groups succeeded by the 6m05s monitor snapshot, with 627.269
aggregate group-service seconds and a maximum group duration of 38.378s. This
demonstrates overlapping production group service, not an exact CPU-core count.
Scratch peak now includes up-front output reservations, so it is not directly
comparable to r7's incrementally charged bytes or physical NFS allocation.
All 376,346,710 records and 256 ranges completed. The remaining catalog work
continued until timeout 124; total wall time including cleanup was 10m05.29s.
There were zero swaps and 121 monitor snapshots. The artifact manifest verified,
scratch was empty, and the unchanged writer remained healthy at epoch 39 with
zero active transactions/intents.

This establishes completion of merge preparation within the cap, not a measured
complete-check speedup. Both runs remain incomplete SlateDB-only diagnostics,
and sequential cache effects are not controlled. The remaining serial ordered
iterator and per-pack aggregation are the next profiling targets; r8 has no CPU
profile to assign their exact costs or distinguish them from catalog scan waits.

### Catalog Attribution and Iterator Follow-Up (r9-r10)

The read-ahead and bounded-merge work above was committed as `8b65896d7` after
fresh affected Go race suites passed. R9 retained the r8 CLI and daemon, adding
loopback-only pprof, five-second CLI thread/goroutine samples, a 30-second CPU
profile after 24 successful merges, and a subsequent three-second Go trace.
Artifacts are under `db.test/phase33-production-2026-09-22-stream32-catalog-profile-r9`;
derived reports have a separate `analysis/SHA256SUMS`, preserving the raw manifest.

The CPU window at 22:11:08 UTC captured 29.11 CPU seconds in 30 seconds:

| CPU path | Share of sampled CPU |
| --- | ---: |
| Pack contribution iterator, cumulative | 77.74% |
| Ordered location iterator, cumulative | 76.57% |
| Encrypted run reader, cumulative | 45.76% |
| Heap pop plus push, cumulative | 27.13% |
| AES-GCM Open, cumulative | 25.21% |
| Allocator, cumulative | 11.61% |

These overlapping percentages must not be added. The trace attributed 602.52 ms
to scratch read syscalls under the catalog iterator, 0.329 ms to network-read
blocking and 13.141 ms to scheduler delay across goroutines. This short window
supports CPU work as the primary target with material filesystem time; it is not
a complete accounting of every async/network wait. R9 timed out in catalog work,
used 2,141.01 CLI CPU seconds and 115,371,828 KiB peak RSS, and took 10m08.46s
including cleanup. The sampler reported no errors; writer health, scratch cleanup
and both artifact manifests verified. Profiling overhead precludes treating r9
as a matched lightweight timing control.

The next candidate changes only the shared scratch iterator hot path:

- Replace per-tuple heap pop/push with root replacement and one sift-down; retain
  normal pop when the contributing reader reaches EOF.
- Give each run reader private reusable length, nonce and ciphertext buffers;
  authenticate/decrypt in place, then decode into a tuple whose fields are copied
  by value. Record format, nonce sequence, authentication and length checks remain
  unchanged. No per-record allocation is needed on the successful read path.

Three benchmark repetitions measured 32-run in-memory merge time falling from
7.425-7.636 ms to 6.177-6.239 ms per 32,768 tuples, about 17%. Reading 4,096
encrypted records fell from 1.862-1.871 ms to 1.092-1.143 ms; allocations fell
from 16,395 to 11, with bytes allocated falling from about 1.968 MB to 1.050 MB
(including the reader's 1 MiB I/O buffer). These isolate mechanisms, not complete
checker speedup. Existing ordering, deduplication, multiset, corruption,
truncation, cancellation and parallel cleanup tests pass, along with full affected
maintenance/telemetry/index-command race suites and editor diagnostics.

R10 uses this candidate with lightweight monitoring, no profiling, unchanged
daemon and the same NFS paths, 96 GiB limits, 32 workers and ten-minute cap.
No builds or tests overlapped. Candidate CLI
`bin/phase33-iterator/linux-amd64/vaultic` SHA-256 is
`c6d98d7bd6fa6b0de9e09724458b34fecc0fffc28383b07f5587e118f46effb6`;
artifacts are under `db.test/phase33-production-2026-09-22-stream32-iterator-r10`.

| Measurement | r8 baseline | r10 iterator candidate |
| --- | ---: | ---: |
| Audit / scan / finalization | 48s / 115s / 28s | 52s / 119s / 33s |
| Catalog stage starts | 3m11s | 3m24s |
| First 24 merges successful by snapshot | 6m05s | 6m05s |
| Total merge calls at cancellation | 24 | 36 |
| Successful / canceled merge groups | 24 / 0 | 32 / 4 |
| CLI CPU seconds | 2,183.94 | 2,023.14 |
| Peak RSS, KiB | 115,318,008 | 115,413,100 |
| Scratch reservation peak, bytes | 90,567,039,078 | 90,567,039,078 |

R10 completed `checkPackCatalog` and entered the subsequent location-count spool
merge by the 9m25s snapshot, when successful groups first exceeded 24. The
`catalog_join` progress label covers both operations, so it does not expose this
boundary directly. The control remained in the catalog comparison at its cap.
R10 used 7.4% less CLI CPU despite reaching later work. Its 32 successful groups
accumulated 684.207 service seconds; four groups were canceled by the cap.
All 376,346,710 records and 256 scan ranges completed, but the later location
count did not finish and its result field remains zero, not a valid count.

Exit was 124; wall time including cleanup was 10m08.96s, with zero swaps and 121
monitor snapshots. Manifest and empty scratch checks passed; the writer remained
healthy at epoch 39 with zero active transactions/intents. Service binaries were
not changed. These results support the iterator optimization but do not establish
complete-check runtime, repeated performance acceptance or a full differential
clean verdict. The next target is the subsequent location-count pass and its
remaining serial merge, with clearer stage attribution before further parallelism.

### Avoid the Count-Only Location Spool (r11-r12)

The iterator follow-up above was committed as `cfc301939`, including the r9
attribution and r10 production evidence. Inspection of the next bottleneck found
that SlateDB-only checking builds a complete blob-ordered spool solely to count
distinct tuples after catalog comparison. Full differential checking still needs
that spool to compare legacy and authoritative locations.

The new candidate counts distinct locations within each blob during the existing
partition scan, only in SlateDB-only mode. It validates blob key kind, partition
prefix and strictly increasing IDs across callback/page boundaries; therefore
the same blob cannot contribute twice. Every location field participates in
per-blob deduplication. Single-location records need no deduplication map, and
multi-location maps live only for the current decoded record. Partition-local
counts avoid shared per-record atomics and are combined with overflow checks
only after successful scan, finalization and adoption. Errors do not publish a
partial count. The unused blob-ordered spool stays empty, eliminating its sort,
spill, encryption, merge and readback work. Pack-contribution tuples retain their
original multiset, including duplicates; full differential and legacy-only
counting retain their existing spool paths.

Tests compare the new count against the old spilled/deduplicated spool across
partitions, duplicates and differences in every location field, and require exact
pack-multiset equality. Separate cases cover empty/singleton scans, duplicate and
reversed IDs across callbacks, wrong prefixes/kinds, malformed values and
cancellation without partial publication. Existing concurrent scan/finalization
tests and all maintenance, telemetry and index-command race suites pass. Editor
diagnostics and diff checks are clean.

Both diagnostics used the unchanged epoch-39 daemon, the same HDD-NFS paths,
96 GiB memory/scratch limits, 32 scan workers/RPCs and ten-minute cap. There was
no profiling or overlapping build/test work. The separate CLI
`bin/phase33-count-only/linux-amd64/vaultic` has SHA-256
`7dd0d82ded80972b0437f87e679639a1e3f0108a8be93eddd20e89527d52b3ff`.
Artifacts are under `db.test/phase33-production-2026-09-22-stream32-count-only-r11`
and `db.test/phase33-production-2026-09-22-stream32-count-only-r12`.

| Measurement | r10 prior iterator | r11 count-only | r12 unchanged repeat |
| --- | ---: | ---: | ---: |
| Encryption audit | 52s | 219s | 52s |
| Location scan | 119s | 91s | 94s |
| Finalization | 33s | 15s | 16s |
| Catalog starts | 3m24s | 5m25s | 2m42s |
| Parallel validation starts | Not reached | Not reached | 7m42s |
| Exit / total wall including cleanup | 124 / 10m08.96s | 124 / 10m01.88s | 0 / 8m12.27s |
| CLI CPU seconds | 2,023.14 | 1,474.18 | 1,499.19 |
| Peak RSS, KiB | 115,413,100 | 57,757,532 | 57,886,232 |
| Scratch reservation peak, bytes | 90,567,039,078 | 48,774,247,512 | 48,774,247,512 |
| Merge groups started | 36 | 24 | 24 |

R11's unusually slow unchanged encryption audit consumed the saved time; it
timed out during catalog comparison and did not publish its computed location
count. R12 completed the reduced-coverage command successfully, reporting
379,934,385 distinct locations from 376,346,710 blob records across all 256
ranges. All 24 merges succeeded by the 4m55s snapshot, totaling 445.132 group
service seconds. Parallel validation took about 29s, reaching finalization at
8m11s. Relative to r10, r12's scan was 21% shorter and finalization 52% shorter;
CPU was 26% lower while completing later work. Peak RSS fell about 50%, and
scratch reservation peak fell 46%. These resource reductions are consequences
of eliminating unnecessary work, not resource-minimization goals.

Both manifests verified, scratch was empty and the writer remained healthy with
zero active transactions/intents. Neither run swapped. R11 exported 121 monitor
snapshots; r12 exported 100. Service binaries were not replaced.

R12 is the first completed SlateDB-only run in this sequence, not a full
differential clean verdict: `coverage.complete` remains false and legacy indexes,
legacy snapshots and export provenance are skipped. The result also reports
419,530 warnings, with pending-export, unknown-tier, retention-unknown and
usage-unaccounted counters each at 419,530; exit zero does not mean warning-free.
Sequential cache effects and r11's audit variability remain confounders, and no
complete baseline runtime exists for a total-runtime speedup percentage.

The remaining dominant interval is catalog preparation/comparison: about 2m13s
to complete the 24 merge groups, then approximately 2m47s for the ordered catalog
comparison and subsequent cleanup/aggregate checks. Parallel validation is much
smaller. Further optimization should target this measured interval while keeping
the full differential-check path covered; it should not infer performance from
raising scan concurrency or from the count-only shortcut alone.

### Compact Pack Contributions Before Merging (r13-r14)

The count-only optimization was committed as `c6deb2e05` with the r11/r12 evidence.
The next candidate removes redundant catalog work: the catalog consumes per-pack
count, payload sum, type flags and presence, but previously merged every blob
contribution individually before computing those fields.

An explicit pack-summary mode now compacts the authoritative pack-contribution
spool's sorted buffers and merge outputs by pack ID. Private temporary tuples
encode partial count, payload, type flags and presence using the existing
authenticated scratch format; they are not repository records. Duplicates add to
counts and payload rather than being discarded. Count and payload addition are
overflow-checked at every combining boundary, and type/presence flags are ORed.
The final iterator combines partial summaries using the same arithmetic. Ordinary
location spools and legacy contribution spools remain unchanged; adoption rejects
mismatched modes. The authoritative pack-summary path applies to both full and
SlateDB-only checking, independently of the count-only optimization.

Tests compare exact summaries with the original multiset path through memory-only
and repeated disk merge passes, including duplicate contributions, mixed types,
presence-only packs, reduced output size and reservation cleanup. Count and payload
overflow are rejected. A failed-write/retry test checks that already compacted
buffers cannot double-count on retry. All affected maintenance, telemetry and
index-command race suites pass, as do focused summary tests and editor diagnostics.

R13 and r14 use the same candidate binary, unchanged daemon, HDD-NFS paths,
96 GiB limits and 32 scan workers/RPCs, with lightweight telemetry and a ten-minute
cap. No tests or builds overlapped either run. Candidate
`bin/phase33-pack-summary/linux-amd64/vaultic` SHA-256 is
`e8eb0b690206f738c89d1d1b1d6ea9d1f9750294a6b97bceeb08ec5fca65eb9d`.
Artifacts are under `db.test/phase33-production-2026-09-22-stream32-pack-summary-r13`
and `db.test/phase33-production-2026-09-22-stream32-pack-summary-r14`.

| Measurement | r12 multiset baseline | r13 summaries | r14 unchanged repeat |
| --- | ---: | ---: | ---: |
| Encryption audit | 52s | 50s | 51s |
| Location scan | 94s | 85s | 84s |
| Finalization | 16s | 6s | 8s |
| Catalog interval | 300s | 61s | 61s |
| Parallel validation interval | 29s | 31s | 31s |
| Total wall including cleanup | 8m12.27s | 3m53.40s | 4m01.33s |
| CLI CPU seconds | 1,499.19 | 965.02 | 940.65 |
| Peak RSS, KiB | 57,886,232 | 55,545,860 | 53,129,728 |
| Scratch reservation peak, bytes | 48,774,247,512 | 15,657,805,264 | 15,657,805,264 |
| Successful merge groups | 24 | 24 | 24 |
| Aggregate merge service seconds | 445.132 | 78.871 | 71.569 |

Both candidates exited zero. Mean total runtime was 3m57.365s, 51.8% shorter
than r12; the catalog interval fell 79.7%. Mean CLI CPU fell 36.4%, and scratch
reservation peak fell 67.9%. R14 spent roughly six seconds after the finalization
progress transition, explaining much of its total-wall difference from r13.
Both scanned 376,346,710 records over 256 ranges and reported 379,934,385 distinct
locations. JSON results matched r12 exactly after excluding only `resources`
and `consistency` (resource telemetry and session consistency metadata).

Manifests verified, scratch cleanup succeeded, and the unchanged writer remained
healthy at epoch 39 with no active transactions/intents. There was no swap;
r13/r14 exported 48/49 monitor snapshots. No service binaries were replaced.

This is repeated completed-run evidence for the current SlateDB-only NFS workload,
not full differential or cross-backend acceptance. Sequential cache effects remain
uncontrolled. All 419,530 warnings and the related counters remain unchanged;
coverage is still explicitly incomplete. The scan is now the largest individual
stage at 84-85s, followed by catalog work at 61s and encryption audit at 50-51s.
Further optimization should reattribute these remaining intervals rather than
assuming the earlier per-record merge bottleneck still dominates.

### Bounded Scan-Time Pack Aggregation (r15-r16)

After commit `cbdacda92`, the next candidate aggregates pack contributions in
partition-local maps before appending partial summaries to the encrypted spool.
Half the pack-spool budget remains available for retained partition buffers;
the other half is divided among configured scan workers for active maps. Entry
admission uses a conservative 256-byte allowance, not an exact Go heap/RSS bound.
Small budgets retain the existing tuple path. At the entry limit, the map flushes
exact partial summaries to the existing spill-capable spool; no global per-record
lock is added. Count/payload overflow and cancellation are checked, and failed
flushes are sticky so retry cannot replay partially emitted contributions.

R15 removed scratch I/O but left many retained memory runs for the final ordered
heap. R16 adds a bounded reduction of those memory runs by pack ID before final
iteration. It uses only estimated unused spool headroom, leaves original runs
untouched on cancellation/error or when the map limit is reached, and replaces
them with one sorted summary run only after successful reduction. Disk runs
continue through the existing encrypted merge path. Both changes affect only
authoritative pack summaries, not full-check location comparison semantics.

Tests compare map limits 1, 3 and 128 with the original summary spool through
multiple flush/spill passes, including duplicate/mixed-type contributions. Tests
cover repeated empty flushes, count/payload overflow, cancellation, sticky scratch
failure and reservation cleanup. Memory reduction tests cover exact results,
successful compaction, no headroom, entry-limit fallback and cancellation without
mutating the retained runs. All affected maintenance, telemetry and index-command
race suites pass, along with editor diagnostics and diff checks.

Both diagnostics used the unchanged epoch-39 daemon, HDD-NFS paths, 96 GiB limits,
32 workers/RPCs, lightweight telemetry and ten-minute cap, without overlapping
builds/tests. R15 CLI `bin/phase33-scan-aggregate/linux-amd64/vaultic` SHA-256 is
`9d244f7420ed2e2b9e777720875becfec254e4fcbe3ed04d0f27364bd19fe2a4`.
R16 CLI `bin/phase33-scan-aggregate-reduce/linux-amd64/vaultic` SHA-256 is
`afd0df5f93deb1dd9681ab197d601cb0805605ac8d871ef724175f034dd7495c`.
Artifacts are under `db.test/phase33-production-2026-09-22-stream32-scan-aggregate-r15`
and `db.test/phase33-production-2026-09-22-stream32-scan-aggregate-reduce-r16`.

| Measurement | r13/r14 baseline | r15 aggregation | r16 plus memory reduction |
| --- | ---: | ---: | ---: |
| Audit | 50-51s | 51s | 51s |
| Scan | 84-85s | 83s | 82s |
| Finalization | 6-8s | 2s | 2s |
| Catalog interval | 61s | 73s | 67s |
| Parallel validation | 31s | 34s | 34s |
| Total wall including cleanup | 3m57.365s mean | 4m04.96s | 3m57.55s |
| CLI CPU seconds | 952.835 mean | 828.89 | 813.10 |
| Peak RSS, KiB | 53,129,728-55,545,860 | 14,428,988 | 14,431,300 |
| Scratch reservation peak, bytes | 15,657,805,264 | 0 | 0 |
| Disk merge groups | 24 | 0 | 0 |

Both exited zero and matched r14's logical JSON results exactly after excluding
only `resources` and `consistency`. Both scanned all 376,346,710 blob records in
256 ranges, counted 379,934,385 distinct locations, and retained 419,530 warnings.
Manifests verified, scratch was empty, no swap occurred, and writer health checks
showed zero active transactions/intents. No service binaries were replaced.

R16 reduces CPU about 14.7% versus the prior mean and peak RSS about 73%, while
eliminating scratch I/O for this workload. It does not establish lower runtime:
total wall is essentially unchanged and the catalog interval remains longer.
This is a scalability candidate, not a throughput win. It has one production
measurement in its final form; cache effects and host variability remain
uncontrolled. Full differential and cross-backend acceptance remain pending.
Before claiming further runtime gains, profile the changed catalog path and scan
waits; reduced allocation/I/O alone does not demonstrate a shorter critical path.

### Reattribute and Stream the Catalog (r17-r19)

Bounded aggregation was committed as `caecdd695` after fresh affected race suites
passed. R17 profiled the unchanged r16 CLI with stage-triggered 30-second CPU
windows and subsequent three-second traces for scan and catalog. The sampler no
longer waits for disk merges, which this workload no longer performs. Artifacts
are under `db.test/phase33-production-2026-09-22-stream32-current-profile-r17`;
CPU/wait reports have a separately verified `analysis/SHA256SUMS`.

The scan window captured 263.83 CPU seconds in 30 seconds. Pack-buffer insertion
accounted for 24.48% cumulative CPU, protobuf eager unmarshalling 24.69%, blob
record decoding 15.27% and key parsing 8.41%; these overlapping shares are not
additive. Its trace attributed 53.03 goroutine-seconds of synchronization delay
to `ReadSession.ScanRange` in the three-second window, consistent with substantial
receive-side waiting but not sufficient to identify the daemon's internal wait
owner. No new daemon CPU profile was collected.

The catalog CPU window captured 17.06 CPU seconds, 96.07% cumulatively in
`compactPackMemory`; map lookup accounted for 68.17%. The subsequent trace sampled
a different part of the stage: 1.50 seconds of blocking beneath unary `ScanPage`
calls, with only about 1.77 ms total syscall time. CPU compaction and paginated
remote reads are distinct costs, not percentages of the same wall-time window.
R17 completed in 3m40.36s with unchanged logical results, no sampler errors,
verified manifests/cleanup and healthy writer state. Profiling and sequential
cache variation make it attribution evidence rather than a performance control.

The next candidate switches the catalog's `p:` scan to the existing retained
streaming range API when no legacy contribution iterator is present. It reuses
the same record visitor, transaction, cancellation and validation paths and
retains unary fallback for stores without range support. When a legacy iterator
exists, pagination is retained: missing-pack callbacks can issue nested `Get`
requests, which could deadlock under a single RPC permit held by a range scan.
This avoids widening the change to full differential catalog reads.

Regression tests verify exact pagination/range results and aggregates, range
dispatch, malformed-record rejection, cancellation and unchanged legacy
pagination. All maintenance, telemetry and index-command race suites pass, as
do editor diagnostics and diff checks.

R18/r19 used the same candidate binary, unchanged daemon and HDD-NFS paths,
96 GiB limits, 32 workers/RPCs and lightweight monitoring under the ten-minute
cap, without overlapping builds/tests. Candidate
`bin/phase33-catalog-stream/linux-amd64/vaultic` SHA-256 is
`3113876a877b9dfc7e8dbc998bbcf30213f3706ea6817c9675b54817d3a7b108`.
Artifacts are under `db.test/phase33-production-2026-09-22-stream32-catalog-stream-r18`
and `db.test/phase33-production-2026-09-22-stream32-catalog-stream-r19`.

| Measurement | r16 unary catalog | r18 streaming | r19 unchanged repeat |
| --- | ---: | ---: | ---: |
| Encryption audit | 51s | 50s | 50s |
| Blob scan | 82s | 82s | 83s |
| Finalization | 2s | 2s | 2s |
| Catalog interval | 67s | 22s | 26s |
| Parallel validation | 34s | 31s | 32s |
| Total wall including cleanup | 3m57.55s | 3m09.56s | 3m13.56s |
| CLI CPU seconds | 813.10 | 807.40 | 803.87 |
| Peak RSS, KiB | 14,431,300 | 14,606,612 | 14,137,216 |

Both candidates exited zero, with zero scratch bytes, disk merges or swap.
Mean runtime was 3m11.56s, 19.4% shorter than r16, with catalog time averaging
24s instead of 67s. Logical JSON results matched r16 exactly after excluding
`resources` and `consistency`: 379,934,385 distinct locations and all 419,530
warnings remain unchanged. Shared range telemetry now includes the catalog:
376,766,240 records equals 376,346,710 blob records plus 419,530 pack records;
257 completed ranges means 256 blob partitions plus one catalog range. These
totals must not be compared as if they were blob-only scan measurements.

Both manifests verified, scratch was empty, and writer epoch 39 remained healthy
with zero active transactions/intents. No service binaries were replaced. This
is repeated completed SlateDB-only evidence, not full differential or cross-backend
acceptance; sequential cache effects remain uncontrolled. The scan at 82-83s
and serial encryption audit at 50s are now the largest remaining stages. Further
scan work needs daemon-side attribution, while bounded parallel authentication
remains a separate candidate that must preserve complete object verification.

### Bounded Encryption Audit (r20-r23)

Commit `4de3c0076` records the catalog-streaming implementation and r17-r19
evidence. The next candidate overlaps up to four encryption-audit objects
using bounded futures, without spawning an unbounded task per listed object.
Byte-weighted admission allows 2 GiB of listed ciphertext sizes per audit,
rounded up to 1 MiB units; an oversized object consumes the entire allowance
and runs alone. The allowance accommodates four nominal 256 MiB SSTs including
encryption overhead, unlike a 512 MiB allowance that could serialize them.

Every admitted object retains the existing header classification, readable-key
snapshot, old-key accounting and complete payload authentication. Internal
`_vaultic/` objects remain excluded. Payload authentication errors from `get()`
still fail the audit; classification and body-stream error handling are unchanged.
Completion order changes, so the first reported error need not follow list order.
Rotation retirement continues to require a successful clean audit.

This is an admission bound, not a strict RSS ceiling: backend and crypto buffers
add overhead, listed sizes can become stale, oversized objects exceed the byte
allowance, and concurrent audit calls have independent budgets. Dropping the
audit or encountering an error drops pending I/O futures and their permits;
already-submitted crypto work can finish on the existing executor afterward.

Local validation: all 51 library encryption tests passed, including rotation
retirement across restart. After increasing the allowance from 512 MiB to 2 GiB,
all three focused audit tests passed again. Deterministic gated reads demonstrate
four-way overlap, byte-limited two-way admission, oversized-object exclusivity,
cancellation and read-error cleanup. Mixed encrypted/plaintext/malformed/old-key
fixtures match serial counts, and corruption in the final payload byte fails
both serial and parallel audits. Editor diagnostics and whitespace checks pass.

With explicit approval, the three focused audit tests passed again and
`RUSTFLAGS="-C force-frame-pointers=yes" make vaulticdb BIN_DIR=bin/phase33-audit-parallel`
built the static release candidate. After a fresh serial-audit control (r20),
only the service daemon was replaced through the configured clean shutdown/start
path. PID 100168 / epoch 39 became PID 165437 / epoch 40, read-write with zero
active transactions and write intents. The service CLI was not replaced.

The deployed daemon SHA-256 is
`efb779ff8e9eda3cb267a7cf56ef9b1726e6fc07c6423e91392afacd430e6cfb`.
Rollback is preserved at `bin/phase33-audit-parallel/rollback/vaulticdb`, SHA-256
`2963b4456fe2cd7f1738f19635745060f5f41df307c496a10e54a917f20a4305`.
Deployment source patch, revision, hashes and before/after health are under
`db.test/phase33-audit-parallel-deployment-2026-09-22`; its manifest verifies.

All runs used the unchanged r18/r19 catalog-stream CLI, 96 GiB memory/scratch,
32 scan workers/RPCs, HDD-NFS paths, five-second lightweight telemetry and the
ten-minute TERM cap with 45-second kill grace. No tests/builds overlapped runs,
and no caches were dropped. Artifacts under `db.test/phase33-production-2026-09-22-`
have suffixes `stream32-audit-control-r20`, `stream32-audit-parallel-r21`,
`stream32-audit-parallel-r22` and `stream32-audit-parallel-r23`.

R21 failed closed after 15.93s: a listed compaction metadata object,
`db/compactions/00000000000000000377.compactions`, disappeared before its read.
Main-store puts advanced from 10 to 12 and deletes from 1 to 2 during the attempt;
no SST compactions were recorded. This is consistent with concurrent metadata
maintenance after startup, but does not establish the exact cause or whether
parallel auditing increases race exposure. No missing-object errors were suppressed
and no automatic retry was added. The writer stayed healthy and scratch was empty.
R21 is a failed startup-adjacent attempt, excluded from successful timing averages,
and remains an operational reliability caveat rather than being discarded.

R22 and r23 completed on the same daemon without another restart:

| Metric | r20 serial control | r22 parallel | r23 parallel |
| --- | ---: | ---: | ---: |
| Exit | 0 | 0 | 0 |
| Wall time | 3m18.70s | 2m45.84s | 2m41.76s |
| Encryption audit interval | 51s | 15s | 15s |
| Blob scan interval | 82s | 84s | 83s |
| Finalize interval | 2s | 2s | 1s |
| Catalog interval | 28s | 26s | 22s |
| Parallel validation interval | 33s | 36s | 38s |
| CLI CPU seconds | 821.00 | 838.55 | 819.48 |
| CLI peak RSS, KiB | 14,988,656 | 14,978,548 | 15,140,192 |
| Daemon CPU seconds | 1,594.03 | 1,585.58 | 1,566.83 |
| Daemon logical read bytes | 102,465,262,687 | 102,465,601,438 | 102,405,566,354 |
| Main-store GET attempts | 73,667 | 73,562 | 73,526 |
| Main-store body bytes | 42,828,492,916 | 42,828,762,623 | 42,771,271,632 |
| Daemon post-run lifetime VmHWM, KiB | 1,386,180 | 1,319,980 | 1,534,288 |

Successful candidate mean wall time is 2m43.80s, 17.6% shorter than the fresh
control. The audit interval fell 70.6%, with roughly unchanged total daemon CPU
and logical reads: this supports overlapping authentication work rather than
skipping it. Stage intervals are rounded progress boundaries; daemon CPU uses
before/after process counters, logical reads are not physical NFS traffic, and
VmHWM is a process-lifetime high-water mark, not a per-stage peak.

R22/r23 logical results match each other exactly after excluding `resources`
and `consistency`. Compared with r20, the sole remaining differing field is
`encrypted_objects`: 161 before restart, 164 afterward. Do not describe that as
exact cross-restart JSON equality. Both retain 379,934,385 distinct locations,
419,530 warnings, and reduced SlateDB-only coverage (`complete=false`). Final
range telemetry remains 376,766,240 records / 257 ranges.

All four raw manifests verified and all scratch directories were empty afterward.
Completed-run telemetry contains 40/33/33 valid snapshots; host and CLI swap stayed
zero. Final writer health is read-write at epoch 40 with zero active transactions
and intents; the candidate remains deployed. Source and evidence remain uncommitted.
These are repeated completed NFS measurements, not full differential or RADOS
acceptance. Restart/cache differences and r21's listing/read race remain unresolved.

### Listing Race Follow-up (2026-09-23, Local Only)

Commit `fb1d589ea` records the bounded parallel audit and r20-r23 evidence.
After the reported host stall, the host was responsive with low load and about
289 GiB available RAM; the same daemon PID 165437 remained read-write at epoch
40 with zero transactions/intents. The operator subsequently identified the host
backup as the cause of the slowdown. No production diagnostic or service restart
was performed during this follow-up; avoid overlapping future timing runs with
the host backup.

A deterministic gated-store regression reproduces deletion between listing and
reading with both one and four audit workers. Both strict audits fail with
`ObjectStore::Error::NotFound`, confirming that this failure mode is not unique
to parallel auditing; its relative production frequency remains unmeasured.

The local read-only encryption-check entry point now permits one fresh full audit
after a typed `NotFound` error. It does not skip the missing object, retain partial
counts, or suppress a second failure. The replacement listing is fully checked,
and backend errors other than `NotFound` and authentication failures still fail
without this retry. Key retirement and capsule migration retain the strict audit
entry point. This is bounded recovery from a non-snapshot listing, not a guarantee
that the object set stays fixed while checking; sustained churn can still fail.
It can roughly double audit work on the retry path, and already-submitted crypto
work from the first attempt may finish after its futures are dropped.

All five focused audit tests pass: serial/parallel disappearance, fresh replacement
listing and counters, second-disappearance failure, nonretryable I/O failure,
payload corruption, admission and cancellation. The daemon binary target passes
`cargo check`; editor diagnostics and whitespace checks are clean. The new retry
has not been deployed or benchmarked, and its source/evidence remain uncommitted.

### Current Scan Attribution (r24, 2026-09-23)

Commit `b4855dd1d` records the check-only retry and the operator's confirmation
that the intervening slowdown was the host backup. R24 did not deploy that retry:
it retained daemon SHA-256 `efb779ff8e9eda3cb267a7cf56ef9b1726e6fc07c6423e91392afacd430e6cfb`,
PID 165437 / epoch 40, and the r18/r19 catalog-stream CLI. Host load was low and
process-name checks found no backup/build/test jobs before the run; these checks
are not proof that no external backup activity occurred.

The ten-minute capped HDD-NFS diagnostic used unchanged 96 GiB limits and 32
workers/RPCs, with no overlapping builds/tests or service changes. The sampler
captured CLI CPU for 30 seconds and daemon CPU at 49 Hz with frame-pointer stacks
during the scan, followed by a three-second Go trace. No scheduler-switch stream
was collected. Artifacts are under
`db.test/phase33-production-2026-09-23-stream32-scan-profile-r24`, with independently
verified raw and `analysis/` manifests.

R24 exited zero in 2m26.87s: audit 15s, scan 81s, finalize 2s, catalog 26s,
parallel validation 20s. CLI CPU was 801.21s and peak RSS 14,524,248 KiB;
daemon before/after CPU was 1,577.60s. These are diagnostic timings, not a matched
speedup result, particularly given the shorter parallel-validation interval.
Thirty telemetry snapshots parsed successfully. Scratch and swap stayed zero,
and final writer health remained read-write with zero transactions/intents.

CPU windows started at 07:30:33 UTC; profile collection/drain ended at 07:31:04.895,
and the subsequent trace ended at 07:31:07.933. The daemon process counters added
576.16 CPU-seconds across approximately 31.6 seconds; the Go CPU profile recorded
275.66 sampled CPU-seconds over 30 seconds. These are different accounting methods
and windows, not directly additive utilization measurements.

Daemon capture contains 25,080 samples, 7.19 MB, and zero reported lost samples.
The sampler logged `perf capture failed` after its deliberate SIGINT stop because
it treated every nonzero wait status as failure. The exact exit status was not
retained, but the capture decodes successfully with a normal perf completion
summary. Raw artifacts remain unchanged; the sampler now records the exit status
and treats requested-interrupt status 130 separately. Some Rust symbols remain
mangled with LLVM suffixes despite the system demangler; names below come from
the identifiable symbol components.

The dominant daemon CPU path is SlateDB iteration, not encryption:

- `DbIterator::next` accounts for 81.98% cumulative sampled CPU.
- `memcpyFast` is 15.73% flat, with stacks through segment/merge iterators.
- One boxed `RowEntryIterator::next` specialization is 10.55% flat; another
  boxed-next entry is 2.78%. These are distinct symbols, not summed cumulative costs.
- `Bytes` shared clone/drop are 3.60% / 2.66% flat.
- jemalloc malloc/deallocation entries are 3.33% / 3.28% flat.
- Merge advance is 3.06% flat; heap pop/push are 2.22% / 1.59% flat.
- The listed AES-GCM assembly hotspot is 1.11% flat, unlike the old amplified
  read/decrypt path. This does not count all crypto cost.

CLI scan CPU remains distributed across pack aggregation (25.19% cumulative),
protobuf eager unmarshalling (24.01%), blob decoding (13.99%) and key parsing
(8.64%); cumulative costs overlap. In the later three-second trace,
48.93 aggregate goroutine-seconds are under gRPC receive and 48.96 under
`ReadSession::ScanRange`. That identifies the CLI's wait location, not the daemon's
async wait cause. Rust iterator/channel/crypto-queue waits are still not separately
measured; CPU stacks alone cannot supply that attribution.

Compared with r23, all logical result fields match after excluding resources,
consistency and the explicitly reported encrypted-object count (164 -> 161).
Distinct locations remain 379,934,385 and warnings 419,530. Coverage remains
SlateDB-only and incomplete; this is not full differential or RADOS acceptance.

The next discriminating experiment should benchmark the pinned SlateDB iterator's
per-row boxed-future, row-copy and heap-advance costs with exact ordered-output
equivalence before changing the dependency. The current evidence does not justify
another read-ahead change or blindly increasing scan workers. No further source
optimization or daemon deployment was made during r24 attribution.

### Local Merge Experiment (2026-09-23)

The operator authorized changes in the SlateDB fork. The supplied
`/root/proj/slatedb` path did not exist in this environment. The clean checkout at
`/root/proj/slatedb-phase32-p2` matched the pinned `f549d4a` revision in
`otuschhoff/slatedb`, so the experiment used that checkout.

The merge iterator combines ordered input streams with a heap, a structure that
keeps the next row at its root. Its old advance path pushed an updated input into
the heap, then immediately popped the next input. The candidate compares the
updated input with the root instead. If the root comes first or compares equal,
the candidate swaps the inputs and lets `peek_mut` repair the heap once.
Otherwise, it keeps the updated input without a heap change. Exhausted inputs
still select the next input with `pop`. No heap guard spans an await.

The patch changes only `slatedb/src/merge_iterator.rs` in the fork. It keeps the
existing key/sequence comparator, duplicate barriers, byte accounting, public
interfaces, and seek path. It does not address boxed-future allocations or change
payload storage. Those costs remain separate targets.

An ignored release-mode test measures one, six, and 32 input streams. Each case
uses 120,000 rows with 34-byte keys and 56-byte values. The fixture uses RAM,
not local disk as a replacement for NFS or RADOS. Fixture creation and iterator
initialization occur before timing. The timed loop consumes and drops every row
through the existing asynchronous interface. One warm-up round precedes ten
measured rounds per case. The probe lives beside the private iterator tests to
avoid adding a public benchmark interface.

The initial unpinned 32-stream interleaved result regressed from 505.30 to
683.98 ns/row. Later unpinned runs did not reproduce that result. Both results
remain in the raw logs. To reduce migration noise, six further runs used CPU 0
in baseline/candidate/candidate/baseline/baseline/candidate order.
The following values are means of three per-run medians, not confidence bounds:

| Input streams | Key layout | Baseline ns/row | Candidate ns/row | Time reduction |
| ---: | --- | ---: | ---: | ---: |
| 1 | Interleaved | 228.97 | 197.43 | 13.8% |
| 1 | Disjoint | 235.08 | 202.20 | 14.0% |
| 6 | Interleaved | 288.44 | 260.49 | 9.7% |
| 6 | Disjoint | 276.75 | 207.29 | 25.1% |
| 32 | Interleaved | 539.90 | 503.66 | 6.7% |
| 32 | Disjoint | 306.53 | 201.15 | 34.4% |

The fixture does not model the full SlateDB iterator stack, encryption, storage
latency, or concurrent scans. Native release builds use a different allocator
and link target from the production daemon. These results support a candidate
for further measurement, not an end-to-end performance claim.

A sorted-reference test compares every returned row across one, six, and 32
streams in both directions. It covers values, tombstones, merge operands,
duplicate suppression enabled/disabled, empty child streams, and exact processed
byte totals. Existing seek and duplicate tests also pass. The broader iterator
suite passes 164 tests, with the timing probe ignored. All 41 snapshot tests pass,
including snapshot reads across compaction.

All 63 Vaultic storage tests pass against the local fork through command-only
Cargo `[patch]` overrides. They include persisted streaming scans, cursor bounds,
snapshot expiry, encryption, and cleanup. An earlier compile used Cargo's legacy
path override and emitted a dependency-resolution warning. The test run used the
recommended patch mechanism instead. Its script preserves and restores the exact
original generated lockfile. Vaultic's committed dependency pin is unchanged.

Artifacts reside at `db.test/phase33-merge-iterator-2026-09-23`. They include saved
baseline/candidate executables, timing logs, source patches, revision, compiler
version, test logs, and the integration script. A SHA-256 manifest covers these
files. Rustfmt and whitespace checks pass. SlateDB commit
`67abacef0044f5e285c1f59af6c936de4c051954` records the tested patch on
`vaultic-multiget-rebase`. Vaultic's dependency pin remains unchanged.
No production service replacement or production diagnostic occurred during this
local experiment.

The next gate is a matched end-to-end scan experiment with an approved candidate
daemon. Keep the existing worker count and read-ahead configuration for that test.
Do not infer a total runtime gain from these isolated merge-loop timings.

### Matched Merge Runs (r25-r28, 2026-09-23)

With operator approval, four HDD-NFS checks compared the committed merge change
in control/candidate/candidate/control order. Each run started after a daemon
restart and a read-write, idle-writer health gate. No caches were dropped. The
same catalog-stream CLI used 32 workers/RPCs, 96 GiB limits, and unchanged
read-ahead. Each check had a ten-minute TERM cap and 45-second kill grace.
No builds or tests overlapped the checks. Process-name checks found no backup
jobs, but cannot exclude external backup activity.

Both static musl daemons used Vaultic `7b46cd41d`, the production frame-pointer
build flags, and the same compiler and allocator. Both therefore included the
check-only audit retry, unlike the previously deployed daemon. Control used
SlateDB `f549d4a`. Candidate used the clean local checkout of committed
`67abacef0044f5e285c1f59af6c936de4c051954` through Cargo patch overrides.
Generated lockfiles differ only in the three SlateDB package sources.
Tracked dependency pins and lockfiles remain unchanged.

The workspace filesystem was full. Source snapshots, build caches, and temporary
executables used RAM. Build artifacts and all diagnostic output were saved on
NFS. Repository data and check scratch stayed on HDD-NFS. A temporary systemd
override selected each daemon without overwriting the installed binary.

| Run | Variant | Total | Audit | Scan | Catalog | Daemon CPU-seconds |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| r25 | Control | 2m34.93s | 30s | 82s | 19s | 1708.01 |
| r26 | Candidate | 2m23.99s | 15s | 80s | 24s | 1553.67 |
| r27 | Candidate | 2m41.61s | 31s | 80s | 27s | 1624.07 |
| r28 | Control | 2m40.29s | 31s | 83s | 20s | 1721.91 |

Stage durations come from rounded progress timestamps. Mean total time fell from
157.61 to 152.80 seconds, or 3.05%. Mean scan time fell from 82.5 to 80 seconds,
or 3.03%. Mean per-run scan throughput rose from 4.567 to 4.710 million records/s.
Mean daemon CPU fell from 1714.96 to 1588.87 CPU-seconds, or 7.35%. Mean CLI CPU
rose from 804.405 to 808.675 CPU-seconds, or 0.53%.

The two runs per variant do not establish confidence bounds. Candidate r27 was
slower overall than either control. Audit and catalog variation exceed the scan
wall-time gain. Whole-check CPU also includes the variable audit work. These
results support a modest scan improvement, not a stable 3% total-runtime promise.
The available counters do not establish the cause of the audit variation.

Accumulated iterator service time fell from a mean 1920.70 to 1786.77 seconds,
or 6.97%. Client receive time fell from 2039.58 to 1968.91 seconds, or 3.47%.
Consumer time fell from 513.67 to 504.19 seconds. These values sum concurrent
range durations. They are not wall time or independent CPU measurements.

All four checks exited zero. All logical result fields match after excluding
resources, consistency session IDs, and encrypted-object counts. The analysis
also compares the remaining consistency fields and engine configuration exactly.
Each run scanned 376,766,240 records in 37,811 chunks across 257 ranges and
reported 379,934,385 locations and 419,530 warnings. Scanned byte totals vary by
five bytes across runs. Encrypted-object counts are 164/169/169/169. These are
live-maintenance observations, not a byte-identical immutable fixture.
Coverage remains SlateDB-only and incomplete, not full differential or RADOS
acceptance. Scratch and daemon swap stayed zero.

Each scan captured daemon CPU at 49 Hz and CLI CPU for 30 seconds, followed by
a three-second Go trace. All daemon captures report zero lost samples and
expected interrupt status 130. Control heap pop/push symbols total 3.59%/3.77%
flat sampled CPU. Candidate `PeekMut` heap repair is 0.86%/1.02%. The main merge
advance symbol rises from 2.97%/2.87% to 4.49%/4.55%, so some work moves there.
`memcpyFast` falls from 15.82%/15.66% to 14.32%/14.63%. These percentages describe
sampled symbols, not isolated operation timings or allocation counts.

SlateDB iteration still dominates daemon CPU. The largest boxed-next symbol is
10.29-11.02% flat across the runs. CLI traces show 49.46-52.23 aggregate
goroutine-seconds in gRPC receive during each three-second window. This identifies
the client wait location, not the daemon's asynchronous wait cause. Iterator
allocation and row copying remain useful next targets. No scheduler-switch or
separate Rust future-wait trace was collected.

Artifacts are under `db.test/phase33-merge-e2e-2026-09-23` and the four
`db.test/phase33-production-2026-09-23-stream32-merge-{variant}-r{25..28}`
directories. They preserve build/run/analysis scripts, source revisions,
lockfiles, executables, raw profiles, decoded reports, and structured comparisons.
Each run's manifest verifies. The harness records the actual `/proc/PID/exe`
hash, rather than the installed binary path, to identify temporary overrides.
Control SHA-256 is
`4160b8ce4ecea8926fb29eacff010e18552e17e7493dcd15f349e98f23487edd`.
Candidate SHA-256 is
`40458409358812e17c51561a22657237e382c389e90cf191fbfa801b95d6adf1`.

The runner removed its override and restored the original daemon after r28.
Binary comparison confirms SHA-256
`efb779ff8e9eda3cb267a7cf56ef9b1726e6fc07c6423e91392afacd430e6cfb`.
Final PID is 268961, epoch 45, read-write with zero transactions and write intents.
No permanent deployment or dependency update was made.

### Local Boxed-Next Experiment (2026-09-23)

The next local experiment targets the boxed iterator adapters in SlateDB
`slatedb/src/iter.rs`. Their async `next` methods create an outer boxed future,
an allocated object that represents pending asynchronous work. The inner iterator
already returns a boxed future. The candidate returns that inner future directly
for both `Box<dyn RowEntryIterator>` and `Box<dyn TrackedRowEntryIterator>`.
It retains the existing async-trait lifetime and Send contract. Initialization,
seek, comparison rules, row storage, and public interfaces stay unchanged.

Two regression tests compare the returned future's address with the inner
allocation through nested boxes. Both use borrowed state and a future that first
returns Pending. They cover polling, cancellation while pending, future drop,
error propagation, and tracked-byte forwarding. The returned future is the same
allocation, not another wrapper. This does not measure all iterator allocations
or eliminate boxing inside the concrete iterators.

Both release test binaries were built at the same source path, with baseline
`67abacef0044f5e285c1f59af6c936de4c051954` and the candidate patch applied afterward.
The existing RAM-only merge probe is unchanged: 120,000 rows, 34-byte keys,
56-byte values, one warm-up round, and ten measured rounds. Builds finished
before timing. CPU 0 ran baseline/candidate/candidate/baseline/baseline/candidate.
Values below are means of three per-run medians:

| Input streams | Key layout | Baseline ns/row | Candidate ns/row | Time reduction |
| ---: | --- | ---: | ---: | ---: |
| 1 | Interleaved | 203.15 | 178.98 | 11.90% |
| 1 | Disjoint | 206.05 | 178.83 | 13.21% |
| 6 | Interleaved | 263.26 | 232.37 | 11.73% |
| 6 | Disjoint | 208.28 | 181.54 | 12.84% |
| 32 | Interleaved | 533.12 | 492.49 | 7.62% |
| 32 | Disjoint | 200.01 | 173.17 | 13.42% |

Raw logs retain timing outliers, including baseline round maxima of 747.54 and
772.50 ns/row. The means above are not confidence bounds. These gains are relative
to the committed heap optimization, not the original push/pop implementation.
The probe does not model production I/O, concurrent scans, or the full iterator
stack. No new production runtime gain is established by this experiment.

Validation passes 13 focused merge tests, two direct-forwarding tests, 164 broader
iterator tests, 41 snapshot tests, and 169 compaction-filtered tests. These suites
overlap and must not be added into a unique test count. The first compaction run
had six failures because the persistent terminal retained a removed temporary
directory in `TMPDIR`. All 169 tests passed after setting the current RAM path
explicitly. Both logs are retained. No source change was needed for that failure.

All 63 Vaultic storage tests pass against the candidate through Cargo patch
overrides in a RAM source snapshot. This leaves the workspace manifest and
lockfile untouched. Rustfmt, editor diagnostics, and whitespace checks pass.
No tests or builds overlapped a production diagnostic.

Artifacts are under `db.test/phase33-boxed-next-2026-09-23`. They include build and
integration scripts, baseline revision, candidate patch, compiler version, both
test executables, raw timings, and validation logs. SlateDB commit
`43a562e3637d3d9ce12bec82a5b9e3deb47843fa` records the tested patch on
`vaultic-multiget-rebase`. Production stays on the restored original daemon. A matched
end-to-end comparison is still needed before a deployment decision for this patch.

### Matched Boxed-Next Runs (r29-r32, 2026-09-23)

With operator approval, four HDD-NFS checks compared heap-only SlateDB `67abace`
against direct future forwarding in `43a562e`. Both static musl daemons used
Vaultic `d9e4b9fd1`, the same source paths, frame-pointer flags, compiler, and
allocator. Both used identical Cargo patch overrides and generated lockfiles.
The build refreshed the changed source timestamp after archive extraction, and
the logs confirm that Cargo rebuilt SlateDB for the candidate. Binary hashes
differ. Tracked manifests and lockfiles remain unchanged.

The experiment retained the catalog-stream CLI, 32 workers/RPCs, 96 GiB limits,
and existing read-ahead. Control/candidate/candidate/control runs each started
after a daemon restart and an idle-writer health gate. Each check had a ten-minute
TERM cap with 45-second kill grace. Builds completed before measurement. No caches
were dropped. Process-name gates found no backup or build jobs, but cannot exclude
external backup activity. Temporary build output used RAM. Repository data and
check scratch stayed on HDD-NFS, with evidence saved on NFS.

| Run | Variant | Total | Audit | Scan | Catalog | Daemon CPU-seconds |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| r29 | Control | 2m38.41s | 29s | 82s | 22s | 1623.44 |
| r30 | Candidate | 2m11.20s | 16s | 67s | 22s | 1111.36 |
| r31 | Candidate | 2m25.42s | 31s | 67s | 22s | 1172.35 |
| r32 | Control | 2m30.99s | 16s | 78s | 31s | 1497.74 |

Stage durations come from rounded progress timestamps. Mean scan time fell from
80 to 67 seconds, or 16.25%. Mean per-run scan throughput rose from 4.713 to
5.623 million records/s, or 19.33%. Mean total time fell from 154.70 to 138.31
seconds, or 10.59%. Both candidate runs finished faster than either control.
Audit and catalog times still vary, and two runs per variant do not establish
confidence bounds. These are measured results for this workload, not guarantees
for other repositories or storage backends.

Mean whole-check daemon CPU fell from 1560.590 to 1141.855 CPU-seconds, or 26.83%.
Mean CLI CPU rose from 830.995 to 853.960 CPU-seconds, or 2.76%. Iterator service
time fell from 1746.35 to 1230.49 accumulated seconds, or 29.54%. Client receive
time fell from 1928.50 to 1552.21 accumulated seconds, or 19.51%. Consumer time
fell from 534.74 to 524.28 seconds. Accumulated range times overlap and are not
independent CPU measurements. Whole-check CPU includes the variable audit work.

All four checks and the runner exited zero. Logical result fields match after
excluding resources, consistency session IDs, and encrypted-object counts. The
remaining consistency fields and recorded engine configuration match exactly.
Each run scanned 376,766,240 records in 37,811 chunks across 257 ranges and
reported 379,934,385 locations and 419,530 warnings. Byte totals vary by 25 bytes.
Encrypted-object counts are 164/169/172/174. This is a live repository, not an
immutable byte-identical fixture. Coverage remains incomplete and SlateDB-only,
not full differential or RADOS acceptance. Scratch and daemon swap stayed zero.

Each scan captured daemon CPU at 49 Hz and CLI CPU for 30 seconds, followed by
a three-second Go trace. Daemon sample counts are 24,558/20,374/19,986/24,339,
with zero reported lost samples. All captures report expected interrupt status
130, and sampler error logs are empty. Daemon CPU counters across the roughly
31.6-second capture windows are 561.03/464.56/461.63/560.41 CPU-seconds.

The largest control boxed-next symbol accounts for 11.12%/10.56% flat sampled
CPU. Candidate boxed-next entries peak at 2.15% in both runs. These are different
remaining symbols, not the same wrapper with a lower cost. `DbIterator::next`
falls from 81.47%/80.86% cumulative samples to 72.58%/73.08%. `memcpyFast` rises
from 14.35%/14.89% to 16.61%/16.54% of samples. The percentage rise does not
establish an absolute increase in copying, because the candidate consumes less
CPU and processes more rows during the fixed window. No allocation counts were
collected in production.

CLI CPU profiles contain 295.99/353.29/356.04/292.26 sampled CPU-seconds in their
30-second windows. Pack aggregation remains about 25% cumulative sampled CPU.
The later three-second traces contain 47.13-51.51 aggregate goroutine-seconds
under gRPC receive. These short windows do not represent total run wait time.
They identify the client wait location, not the daemon's asynchronous wait cause.
Separate Rust future-wait and scheduler-switch attribution remains unavailable.
Row copying and the remaining iterator allocations are further investigation
targets, not changes included in this experiment.

Artifacts reside under `db.test/phase33-boxed-next-e2e-2026-09-23` and the four
`db.test/phase33-production-2026-09-23-stream32-boxed-next-{variant}-r{29..32}`
directories. They include build/run/analysis scripts, revisions, lockfiles,
executables, raw profiles, decoded reports, and structured comparisons. All four
raw run manifests verify. Control SHA-256 is
`cb710d2a779f2dc67a1ecda9ed4f07e9e220495b0c6df8a303877346bf228cb7`.
Candidate SHA-256 is
`6b00ab81536468897b2e38fa458ed86a39c201464d036608e19afc72e2fc1014`.

The runner removed its temporary override and restored the original daemon.
Binary comparison confirms SHA-256
`efb779ff8e9eda3cb267a7cf56ef9b1726e6fc07c6423e91392afacd430e6cfb`.
Final PID is 385190, epoch 50, read-write with zero transactions and write intents.
No permanent deployment, dependency update, commit, or push occurred during this
experiment. The result supports adopting the candidate for this NFS workflow,
subject to an explicit dependency and deployment decision.

### NFS Adoption (r33, 2026-09-23)

The user approved adoption after the matched comparison. The SlateDB fork now
publishes `43a562e3637d3d9ce12bec82a5b9e3deb47843fa` on
`vaultic-multiget-rebase`. Both direct Vaultic dependencies pin that revision.
The lockfile changes only the three SlateDB Git source identities.
Cargo metadata resolves all three packages from the published revision with
`--locked` and no local overrides. All 63 Vaultic storage tests pass against
that locked Git dependency.

The durable service executable now contains the exact candidate from r30/r31:
`6b00ab81536468897b2e38fa458ed86a39c201464d036608e19afc72e2fc1014`.
That binary was built from Vaultic `d9e4b9fd1` with a local Cargo patch to
SlateDB `43a562e`, not rebuilt from the new Git pin. The published-pin tests
provide a separate dependency-resolution gate. Installation used an adjacent
staged file, a graceful service stop, and an atomic rename. The service
configuration and CLI remain unchanged, with no runtime override.

The capped post-deployment check exited zero in 2m23.58s. Approximate audit,
scan, and catalog durations were 31s, 67s, and 22s. The daemon used 1180.46 CPU
seconds and the CLI used 837.38 CPU seconds. This single run had no CPU or trace
profiling and is a deployment check, not another matched performance comparison.
It used the same 32 workers/RPCs, 96 GiB limits, and HDD-backed NFS storage.
No builds, tests, or detected backup processes overlapped the check.

Logical results match r31 under the same dynamic-field exclusions used above.
The remaining consistency fields and engine configuration match. Scan totals
remain 376,766,240 records, 37,811 chunks, and 257 completed ranges. Locations
remain 379,934,385 and warnings remain 419,530. Encrypted objects total 164.
Scratch use and daemon swap are zero. Coverage remains incomplete and
SlateDB-only. This adoption does not close full differential or RADOS acceptance.

Final PID is 391849, epoch 51, read-write with zero transactions and write intents.
The original daemon remains available as `rollback-vaulticdb` under
`db.test/phase33-boxed-next-adoption-2026-09-23`, with SHA-256
`efb779ff8e9eda3cb267a7cf56ef9b1726e6fc07c6423e91392afacd430e6cfb`.
That directory contains the deployment script, rollback gates, health records,
locked-resolution metadata, and test logs. Raw r33 results reside under
`db.test/phase33-production-2026-09-23-stream32-boxed-next-adopted-r33`.

### Full Differential Attempt (r34, 2026-09-23)

The first full differential attempt after adoption reached the ten-minute cap
in `legacy_scan`. It did not reach the encryption audit or SlateDB scan.
The timeout wrapper returned 124. The CLI reported cancellation and completed
cleanup within the 45-second grace period. Total elapsed time was 10m11.50s.
No final comparison summary exists, so this attempt provides no clean verdict.

The command omitted `--slatedb-only` and retained 32 workers/RPCs, 96 GiB memory
and scratch limits, and HDD-backed NFS storage. Scratch reached 64,438,495,400
bytes at the cap. CLI peak RSS was 81,082,900 KiB and CPU time was 1303.56s.
The 122 interval samples averaged 90.88% host idle and 0.44% I/O wait.
These host counters do not isolate legacy parsing, serialization, or NFS latency.
No build, test, or detected backup workload overlapped this measurement.

The final monitor snapshot gives a more specific explanation. Scratch sorting
took 261.16s across 841 completed operations. The 839 completed spill writes
spent 225.40s in encoding/write, 0.078s in final buffered flush, and 59.63s in
file sync. One additional encoding/write operation was cancelled. No merge
completed. These are elapsed stage totals, not CPU or pure device wait time.

`ForAllIndexesWorkers` loads and decodes concurrently but holds one mutex while
calling the legacy consumer. `loadLegacyLocations` sorts and spills both spools
inside that serialized callback. Each spool receives one quarter of the memory
budget, or 24 GiB here. These facts support a bounded parallel legacy-consumer
experiment with private spools. They do not support adding more decoder workers
or treating all scratch time as NFS latency. No legacy behavior changed here.

The daemon remained at PID 391849, epoch 51, read-write with zero transactions
and write intents after cancellation. Scratch cleanup left no files. The raw
manifest verifies under `db.test/phase33-production-2026-09-23-stream32-adopted-full-r34`.
Legacy scan and spill attribution is now the next full-check acceptance blocker.
Do not extend the diagnostic cap or infer success from the reduced checks.

Native RADOS validation remains blocked on this host. The installed daemon is
a generic static build, and the host has no discoverable librados, Ceph CLI,
Docker, or RADOS-enabled binary. The existing integration harness provisions
a disposable container cluster, not the authorized production storage.
No credential contents were exposed and no production backend was changed.

### Scan Wait Diagnostics (r35-r38, 2026-09-23)

An opt-in `VAULTICDB_SCAN_TIMING=1` probe now records one JSON event per scan
stream. It measures elapsed and polling wall time for setup, chunk collection,
transaction validation, and response-channel reservation. Cancellation settles
the active measurement. The event contains no keys, values, or transaction IDs.
The probe is disabled by default and does not change the wire protocol.

The locked published dependency built as a static musl release with the existing
frame-pointer flags. All 63 storage tests passed with timing disabled. All three
persistent-scan tests passed with timing enabled, and all ten attribution tests
passed. Enabled test logs contained 40 valid timing records across success,
error, and cancellation. The unused RADOS test constant warning is unrelated.

Temporary service overrides selected the same instrumented binary for r35-r37.
Each check retained 32 workers/RPCs, 96 GiB limits, and HDD-backed NFS storage.
No builds, tests, or detected backup processes overlapped the measurements.

| Run | Timing | Total | Approximate scan | Daemon CPU seconds | Timing records |
|---|---|---:|---:|---:|---:|
| r35 | Off | 2m24.80s | 66s | 1175.01 | 0 |
| r36 | On | 2m11.31s | 67s | 1101.33 | 257 |
| r37 | On, raw scheduler attempt | 2m11.50s | 67s | 1101.75 | 257 |

The audit took 29s in r35 and 15s in r36/r37. The lower total with timing enabled
is not evidence of a speedup. One off/on pair cannot establish precise probe
overhead. Logical results, scan counts, and configuration match r33 under the
same dynamic-field exclusions. Every enabled production timing record reports
success. The scan still returns 376,766,240 records in 37,811 chunks and 257 ranges.

Across the 257 r36 streams, setup elapsed time totals 20.05s, with 0.17s inside
polls. Chunk collection totals 1212.50s, with 829.34s inside polls and 383.16s
between polls. Delivery reservation totals 733.75s, almost entirely between
polls. Transaction validation totals 93.53s, with 0.15s inside polls.
These concurrent durations overlap and are not command wall time. Polling wall
time includes preemption and synchronous blocking, not just CPU execution.
SlateDB explicitly consumes Tokio's cooperative budget during iteration, so
the 2,954,179 pending collection polls include fairness yields, not only I/O.
Delivery time identifies downstream backpressure but not its client-side cause.

The raw scheduler attempt in r37 produced metadata but no switch/wakeup samples.
Its local-thread filters are not validated in this PID namespace. Those artifacts
remain available, but they provide no scheduler-delay evidence.
The runner then restored the adopted daemon and removed its override.

R38 used the adopted binary without a restart or source change. A separate
30-second process-scoped perf capture recorded context switches with namespace-safe
attachment. The check exited zero in 2m05.99s and matched r33 logical results,
scan counts, and check configuration. This is not a matched runtime comparison.
The trace contains 3,511,260 records over a 29.46s event window, with no lost-record
markers or decoder errors. Complete intervals across 66 observed threads total
439.67s on CPU and 1474.33s off CPU. Preempted switch-outs account for 97.23s of
the off-CPU total. Initial and trailing boundaries are excluded.
These aggregates include idle workers and background tasks. Without wakeup
events, off-CPU time cannot be split into sleeping time and runnable delay.
They are neither asynchronous-task wait time nor critical-path wall time.

The adopted daemon remains active at PID 431717, epoch 55, read-write with zero
transactions and write intents. Its executable matches the saved adopted binary,
and no runtime override remains. The instrumentation is not permanently deployed.
Artifacts, source patch, test/build logs, diagnostic journals, and analyzers reside
under `db.test/phase33-scan-timing-2026-09-23`. Raw checks use
`db.test/phase33-production-2026-09-23-stream32-scan-timing-{off-r35,on-r36,scheduler-r37}`
and `db.test/phase33-production-2026-09-23-stream32-adopted-context-r38`.
The next performance experiment must address the measured serialized legacy
consumer before another full differential acceptance attempt.

## Prior Production Runs

This record captures bounded full and reduced-coverage check attempts against
the activated production takeover on 2026-09-22. It is representative evidence
for the current NFS deployment, but it is not a successful Phase 33 acceptance
run: the first run stopped at metadata encryption and later runs reached but did
not complete the SlateDB location scan.

The immutable raw artifacts are outside the repository at
`/volume2/NASDA2/rustic/db.test/phase33-production-2026-09-22/`; its
`SHA256SUMS` manifest verifies all 14 captured files. The normalized
partial-result SHA-256, with the read-session ID removed, is
`a4ebb3fc50c2472d04afab32afb294ddf0ce0412b737d734a9f08d69912b0b03`.

## Environment and command

- Source repository: `/volume2/NASDA2/rustic/repo`, NFSv3 with 128 KiB reads.
- Authoritative database: `/volume2/NASDA2/rustic/db`, resolving to a separate
  NFSv3 mount with 64 KiB reads.
- Repository identity:
  `c4d68689c785d02a28d6eec485c62132823dc9873ff7f627c2bac80258251528`.
- Database service: `vaulticdb-rustic.service`, read-write writer epoch 35.
- Host: 32 logical CPUs, 300 GiB RAM, no swap.
- Source revision: `451d1f98f11ba2c75f3b57ade0858d5f73a573f2`.
- Retained profile binaries identify themselves as
  `v0.2.11-42-g324b22043-dirty`; results therefore apply to those exact binaries,
  not a reproducible clean build of `HEAD`.
- Checker settings: full differential coverage, 32 workers, 32 RPCs,
  `--check-memory=auto`, 8 GiB scratch limit, encrypted metadata required,
  crawl-debt details enabled, and a ten-minute outer timeout.

The source inventory contained 10,019 index files totaling 19,245,153,330 bytes.
The activated aggregate catalog contains 419,530 packs and 379,934,385 blobs.
No caches were dropped and the production service was not restarted.

## Result

The command exited 1 after 5m30.38s, before the outer timeout:

```text
inventory       0s
legacy_scan     0s .. 5m11s
encryption_audit 5m11s .. 5m20s
failure         CheckEncryption RPC DeadlineExceeded
```

The partial result retained full requested coverage metadata, read session
`txn-4152527-2`, generation 1, and legacy inventory digest
`9983d7d4c41bbb87c2335bf6a40d6ef90a627f2f61c16499e12dd974709f4703`.
It had reached 10,019 legacy indexes with no scratch and no merge passes. It did
not reach `slatedb_scan`, `catalog_join`, `parallel_validation`, or successful
finalization, so zero mismatch counters in this partial JSON are not a clean
verdict.

The same fixed failure had occurred in the preceding activation check. This is
repeatable, not a ten-minute timeout artifact.

## Resource and throughput observations

`/usr/bin/time -v` measured the CLI:

| Metric | Observation |
|---|---:|
| User CPU | 1,139.38 s |
| System CPU | 91.69 s |
| Average CPU | 372% |
| Peak RSS | 98,281,444 KiB (93.7 GiB) |
| Major faults / swap | 0 / 0 |
| Scratch peak | 0 B |

The legacy scan completed in about 311 seconds: 32.2 indexes/s and 59.0 MiB/s of
encoded legacy index input. Dividing the known imported blob cardinality by the
stage duration gives about 1.22 million blob records/s as a scale indicator; it
is not a direct progress counter from the checker.

Host samples averaged 11.42% user CPU, 1.52% system CPU, 86.94% idle, and 0.11%
I/O wait. This warm run therefore did not saturate the host or exhibit an NFS
wait bottleneck during the legacy stage. The CLI nevertheless averaged only
3.72 logical CPUs despite 32 workers. Its 93.7 GiB peak RSS and the fall in free
memory show that auto memory admission is intentionally aggressive on this
large host; no swap or scratch occurred, but an explicit lower production budget
must be tested before accepting that default operationally.

The daemon averaged 2.91% CPU in the host sampler, peaked briefly at 115.9%, and
reached about 517 MiB sampled RSS. Across the run it added about 1.33 GiB of
physical reads and 6.11 GiB of logical reads. System-wide NFSv3 counters rose by
25,841 operations, dominated by 21,734 reads and 1,950 writes. These counters
include unrelated host traffic and cannot attribute requests to one mount or
process; they are supporting context, not proof of service time.

## Bottlenecks and decisions

### Initial P0, resolved: encryption audit could not complete

`CheckEncryption` uses the generic ten-second unary RPC deadline. The server's
`audit_objects` implementation lists every metadata object, downloads each full
object, checks its header, and authenticates encrypted contents before returning
one aggregate response. A 46 GiB NFS-backed store cannot reliably complete that
whole-store operation in ten seconds. The client cancels the request and the
full checker can never progress beyond this stage.

Replace this with a bounded paged/resumable audit tied to the pinned read session,
or introduce an explicit long-operation deadline only as an interim unblocker.
The durable solution must expose object/byte progress, cancellation, continuation
identity, and invalid/plaintext/old-DEK counts without weakening exact coverage.
Do not merely raise the generic deadline for every RPC.

### P1: legacy scan is CPU/allocation limited and memory-heavy

The legacy stage occupies at least 97% of the observed pre-failure wall time.
Host I/O wait is negligible and CPUs remain mostly idle, while the CLI averages
3.72 cores and reaches 93.7 GiB RSS. Existing synthetic profiles already locate
the remaining work in JSON/index decode, packed-blob conversion, tuple storage,
runtime scanning/GC, and serialized insertion into the shared legacy spool.
Production evidence agrees with the low parallel utilization but does not yet
provide symbolized production attribution.

Next compare 4/8/16/32 workers with an explicit memory budget and identical warm
input, at least three repetitions after P0 is fixed. Before adding workers,
partition legacy tuple output per decode worker and merge those sorted runs;
this removes the documented shared-spool mutex boundary. Then profile conversion
and allocation ownership. Accept a change only on end-to-end full-check runtime,
not legacy-stage CPU alone.

### P1: auto memory needs an operational cap

Auto admitted 279,539,385,754 bytes and the partial check retained 93.7 GiB RSS.
That avoided encrypted scratch, but leaves little isolation from colocated work
and makes a later full-check peak unknown. Benchmark explicit 32/64/96 GiB
budgets with local encrypted scratch. Measure elapsed time, peak RSS, spill,
merge passes, and daemon cache effects; choose a production default from that
curve rather than using nearly all reclaimable host memory.

### P2: production observability is below the documented contract

Progress exposes only stage, elapsed time, workers/RPCs, and scratch peak. It
omits records, bytes, RPC count/latency, queue/admission wait, CPU, and memory
high-water. `vaultic monitor` could not attach to the daemon using the available
global metadata-unlock socket option and reported `vaulticdb` unavailable without
a diagnostic. Host sampling was therefore necessary and could not separate the
two NFS mounts.

Add the normal index daemon attachment flags to monitor commands, preserve the
unavailability reason, and publish checker counters from the long-lived process
that owns them. Add stage-local records/bytes and wait-state snapshots before the
next tuning campaign. This is required to distinguish source decode, RPC
admission, daemon service, object-store read, response delivery, and scratch
backpressure.

## Superseded initial experiment order

This initial experiment order has been superseded by the follow-up evidence
below. The audit now completes, range sharding and independent spools are
implemented, and 64 workers do not outperform 32. The next experiment requires
a server-owned resumable range scan or stream that retains iterator state across
bounded response chunks. After focused protocol/session tests, rerun the same
ten-minute SlateDB-only diagnostic at 32 workers. Proceed to a full differential
check only when `slatedb_scan` completes; then profile catalog join and later
validators. Defer broader memory and backend matrices until that exact path can
finish.

No Phase 33 representative-scale acceptance gate is closed by these attempts.

## Follow-up production evidence

The encryption audit received a dedicated one-hour default deadline that
preserves shorter caller deadlines, and its initial object classification was
reduced from a duplicate full-object read to a bounded 38-byte header read. The
optimized full check completed the legacy scan in about 5m11s and the audit in
about 2m24s, then remained in `slatedb_scan` until the ten-minute cap. Its daemon
performed 40.1 GiB of physical reads and 631,052 NFS reads while host I/O wait
averaged 0.59%. The audit blocker is removed; zero partial-result mismatch counts
still are not a clean verdict.

Reduced `--slatedb-only` diagnostics then isolated the database path. These runs
skip legacy indexes, legacy snapshots, and export provenance and therefore are
performance diagnostics, not migration sign-off.

| Variant | Audit reached scan | Result at cap | Main observation |
|---|---:|---|---|
| 1,000-item pages, 32 workers, 64 GiB | 1m03s | scan incomplete | 419 GiB daemon logical reads in ten minutes |
| 10,000-item pages, 32 workers, 64 GiB | 48s | scan incomplete | lower iterator/RPC overhead, still serial client ingestion |
| partitioned scan, shared spools, 32 workers | 50s | 8 GiB scratch exhausted at 8m04s | daemon parallelism unlocked; shared spool serialized output |
| sharded spools, 32 workers, 96 GiB | 51s | scan incomplete; 61.6 GiB scratch | about eight times the prior scratch-output progress |
| sharded/buffered, 32 workers, 96 GiB | 2m16s | scan incomplete; 44.3 GiB scratch | run files fell from about 13,200 to 778 |
| sharded/buffered, 64 workers, 96 GiB | 1m07s | scan incomplete; 44.3 GiB scratch | no gain over 32 workers |

The 32-worker sharded run used 5.97 CLI CPUs on average and the daemon used about
9.7 CPUs. The host still averaged 47% idle and 0.67% I/O wait, but NFS counters
rose by 8.64 million operations, including 8.02 million `getattr` calls. With
64 workers, daemon CPU and completed scratch output did not improve. Current
throughput is therefore limited by SlateDB iterator/table metadata traversal and
the unary page model, not by raw storage bandwidth or lack of checker workers.

The accepted local improvements are:

- a 10,000-item daemon scan-page limit while retaining the 16 MiB response cap;
- 256 disjoint first-ID-byte blob prefixes scanned concurrently within the
   configured worker/RPC limits and the same pinned read session;
- independent bounded location spools per partition, adopted into the exact
   downstream reducers without tuple copies;
- 64 MiB bounded sort chunks and 1 MiB buffered encrypted scratch I/O.

The next optimization must be a server-owned resumable range scan or stream that
keeps iterator state across response boundaries, remains tied to the pinned read
session, has bounded flow control, and exposes records/bytes progress. More
workers, larger pages beyond the message cap, or further scratch tuning are not
supported by the measurements. A full differential clean check remains required
before migration validation can be signed off.
