# Metadata Read Amplification Reduction

Status: opt-in implementation and private correctness qualification; no production deployment.
Date: 2026-11-03.
Related contract: [backup readiness](indexdb-backup-readiness.md).

## Objective

Reduce repeated metadata fetching, decoding and decryption during complete
unchanged-source backups, while preserving repository availability checks,
snapshot correctness, cancellation, memory admission and durability.

The user-facing performance objective is at least 100 observed/scanned entries
per second on representative unchanged `fme_cae1` input. This is a full-backup
objective, not a blob-lookup benchmark objective. Reducing RPC count or hiding
latency alone does not prove reduced read amplification.

A candidate qualifies only if it meets all correctness gates, improves sustained
full-crawl throughput, and reduces the measured work targeted by its hypothesis.
Use the lowest useful concurrency that meets the objective within the approved
resource envelope. Do not select a concurrency peak from a subsecond screen.

## Evidence and Unknowns

The retained 100,000-entry unchanged pass lasted 2,266.464 seconds and achieved
44.122 entries/second. All 89,300 regular files were unchanged; restore and
encrypted reopen passed. Reconciliation waited for prepared work for 96.8% of
the main interval. Blob-size validation issued 78,718 RPCs for 85,078 handles,
averaging 1.08 handles/call and 26.121 ms/call. The CLI size cache had no evictions;
that does not establish that daemon metadata caches were effective.

The metadata object-store wrapper accumulated 2,033.370 worker-seconds of `get`
work and returned 952,145,533,531 logical plaintext bytes to SlateDB. It surrounds
cache access and encryption: these are neither exclusive wall-time shares nor
physical NFS traffic measurements. The current daemon uses 128 MiB of engine
metadata cache and 512 MiB of engine block cache.

An authenticated singleton lookup screen on a separate historical clone used
512 present and 128 absent keys in fixed seeded order. Fresh-engine runs took
10.846, 1.154 and 0.461 seconds at 1, 8 and 32 callers respectively. With one
caller, paired means were 16.907 ms end-to-end, 16.477 ms inside storage,
0.012 ms elsewhere in the handler and 0.417 ms outside the handler. At 32 callers,
median and tail latency increased while aggregate throughput still improved.
This short sequential screen establishes overlap, not a saturation point or
production throughput. Host/NFS caches could already be warm.

The verified 10k unforced retry used a four-RPC cache limiter shared by size
and location lookups. Size calls peaked at two in flight; size-slot waiting
totaled 10 ms. This counter does not cover location-slot waits. Record caller
parallelism, the effective cache limiter, exact size-call peak and sampled
overall RPC concurrency separately; the 32-caller direct screen bypasses this
cache limiter and cannot establish the backup path's concurrency knee.

Retained evidence roots:

- `/volume2/NASDA2/vaultic-test-m6/unchanged-crawl-fme-cae1-20261102-ten-minute`
- `/volume2/NASDA2/vaultic-test-m6/read-wait-fme-cae1-20261103`
- `/volume2/NASDA2/vaultic-test-m6/force-compare-fme-cae1-20261103-10k`

The first root's outer test failed after successful measurement/restore/reopen
because of a stale-client cleanup defect. Its failed receipt remains preserved;
the ownership fix passed a focused native test. Never rewrite it as a full pass.
The third root retains the requested comparison's first failed attempt: its
unforced content checks passed, but an overly strict harness guard rejected
1,449 bytes in four new ancestor-directory tree blobs. `data_added` includes
tree metadata, not just file content. The corrected fresh-leg retry is under
that root's `retry` child and uses the qualified parent/input read-only. Populate
its accepted results only after both exact-scope legs and outer cleanup succeed.

The retry is now accepted: unforced main/command time was 207.267/224.284 seconds
at 48.247 entries/second; forced was 47.484/66.944 seconds at 210.597 entries/second.
Both had zero new data blobs and exact restore/reopen/source verification.
Unforced/forced writer prepared-work waits were 199.777/38.118 seconds, and
logical metadata work was 112.860/98.296 GB. The 4.365-fold main-time contrast
primarily demonstrates overlap rather than elimination of amplification.
Neither leg was publication-dominated on this fixture. All 49 evidence hashes
verified; one short sequential pair does not meet sustained acceptance.

Captured paired header totals include four/three startup reads because the
critical reporter subtracted calls but not the newly added timing fields. The
report labels those means cumulative; unrecorded startup durations are not
invented. The reporter now subtracts all cumulative timing counters from the
same baseline and has a focused race-tested regression. Preserve raw captures.

Unresolved representative-workload questions:

- Which repeated ranges are SST indexes, filters, data blocks or other objects?
- How much time is raw fetching, crypto queueing, authentication/decryption,
  engine decoding, and cache admission or locking?
- Which ranges miss decoded metadata, block, encrypted-object or decrypted caches?
- How much backend traffic reaches NFS, and how much reaches physical storage?
- How do these answers change with concurrency, real batching and cache sizing?

## Implemented Controls

The implementation retains existing defaults. Content readers, unchanged-file
validators and actual metadata RPCs have separate concurrency controls.

| Control | Default | Behavior |
| --- | --- | --- |
| `--unchanged-read-concurrency` | 0 | Synchronous unchanged validation; 1-128 enables a separate bounded pool |
| `--metadata-read-concurrency` | 4 | Shared size/location RPC limiter; maximum 128 |
| `--metadata-batch-delay` | 0 | Optional cross-file coalescing; maximum 5ms and 256 unique handles/batch |
| `--metadata-exact-catalog` | false | Complete pinned-session ID/type/size scan into encrypted admitted working storage |
| `--metadata-daemon-meta-cache-bytes` | 0 | Startup override; zero preserves the daemon's metadata-cache default |
| `--metadata-daemon-block-cache-bytes` | 0 | Startup override; zero preserves the daemon's block-cache default |
| `--metadata-daemon-multi-get` | false | Enables genuine engine MultiGet at startup without enabling bulk-import WAL behavior |

Batching and catalog options require `--metadata-on-demand` and its existing
scratch/cache or shared-working-policy prerequisites. Startup overrides do not
reconfigure an already connected daemon. The CLI size-cache budget remains
distinct from both native engine caches.

Validation jobs and completed pending futures remain charged until completion
and ordered consumption respectively. Cancellation and consumption release
future charges exactly once. Workers preserve source ordering and ordinary
missing-content fallback, recheck source metadata through the configured
filesystem before reuse, and never allow the error callback to suppress a
metadata availability failure. Content-reader queues close only after validator
handoffs finish. The unbuffered validation queue bounds active jobs and the
single traversal waiter; admission also bounds retained results.

Coalescing deduplicates pending handles, preserves requested ordering and
duplicates, and flushes on the configured deadline or the batch bound. Joined
requests share the leader's RPC slot instead of consuming the whole limiter.
Individual caller cancellation does not cancel an owner-session fetch. Batch
buffers remain admitted until all consumers finish, and failed requests never
become cached negatives.

The exact catalog scans `b:` records in pinned 64-record pages and validates the
session before and after loading. No partial scan activates positive or negative
answers. ID and blob type remain distinct; zero sizes are valid. Keys are HMAC
tokens and values are AES-GCM authenticated sizes in the existing RAM/KV/auto
working store. Local writes override the old view, including an exact negative.
Corruption, budget exhaustion, cancellation and dead sessions fail closed.
Catalog entry count, load duration and working-store resources remain reported;
startup and total-command cost must still include the complete load.

Native optional encryption-read telemetry now exposes requested plaintext and
rounded ciphertext bytes, origin calls/bytes/time, encryption-header decoding,
header-cache hits/misses/evictions, crypto queue/service time and read classes.
Crypto queue timing includes semaphore and executor scheduling. Main critical
frames subtract the same valid daemon baseline; missing, partial, null or reset
telemetry is unavailable, not an invented zero. Location-slot wait time is also
reported separately from location RPC elapsed work.

Scope limits: ciphertext-origin reads can be served by an encrypted cache and
are not physical NFS/HDD traffic. Encryption counters can include wrapped WAL
origins and other clients. Header decoding is not full SST decoding; no claim
is made to isolate SST decode or cache-lock time. No trusted plaintext cache,
compaction change, metadata relocation or production configuration is introduced.

## Implementation Qualification

Focused race tests cover overlapping unchanged validation, exact restored
content, missing blobs, source mutation, fatal lookup errors, cancellation,
hardlinks, symlinks, exclusions and drained working reservations. Cache tests
cover enabled batching, shared caller cancellation, Close, duplicate ordering,
bounded batches, failed/partial catalog scans, authenticated malformed sizes,
RAM/KV/auto storage, budget exhaustion and write-overlay precedence. Native
read-session tests exercise MultiGet off/on and a catalog excluding post-session
writes. Injected origin and crypto delays pass two native attribution tests.

The affected index, global and backup-command suites pass race detection.
The full archiver race suite passes with root DAC-bypass capabilities dropped;
the unrestricted root run cannot exercise its unreadable-file permission cases.
Eight private encrypted native workflow cases pass exact restore and reopen:
metadata caches 128/256/512/1024 MiB, coalesced RPC limits 4/8/16, and exact catalog.
These 75-file fixtures check correctness and settings, not throughput gains or
the useful cache/concurrency knee.

`helpers/phase35-m6/metadata_read.py` supervises fresh named 10k `parallel`,
`coalesced` and `catalog` modes using the previously qualified parent/input
read-only. Each captures unforced and forced legs in independent checksum-verified
clones, retains exact restore/reopen/source checks, binary/source pins and process
observations, and bounds RSS, time, fixture bytes and shared free space. Run it in
a private mount namespace with core dumps disabled. It refuses existing evidence
and production activation; captures needing kill fallback are rejected.

The coalesced candidate's supervised 10k pair completed successfully. Its
settings were 16 unchanged validators, eight actual metadata RPC slots, 1ms
batch delay, engine MultiGet enabled, 128 MiB metadata cache and 512 MiB block
cache. Content readers remained 16 and publication groups remained 64.

| Mode | Main Seconds | Command Seconds | Entries/Second | Size RPCs / Handles | Logical Metadata GB |
| --- | ---: | ---: | ---: | ---: | ---: |
| Historical unforced control | 207.267 | 224.284 | 48.247 | 8180 / 9210 | 112.860 |
| Coalesced unforced candidate | 69.457 | 87.463 | 143.973 | 7695 / 9210 | 205.138 |
| Historical forced control | 47.484 | 66.944 | 210.597 | 9210 / 9210 | 98.296 |
| Coalesced forced candidate | 18.158 | 38.298 | 550.732 | 1613 / 9210 | 105.751 |

Both candidate legs observed/scanned all 10,000 entries, processed 9,209 regular
files with the expected force/parent semantics, stored zero new data blobs, and
passed exact restore, encrypted reopen, unchanged-source and idle-writer checks.
Their 1,449 added bytes were ancestor-tree metadata. The outer capture accepted
cleanup, and a subsequent process check found no candidate daemon survivors.
Peak combined owned RSS was 42,029,805,568 bytes; the 1,702.534-second outer time
includes clones, checksum verification and restores, not sustained main backup.

Unforced batching averaged only 1.197 handles/RPC; forced averaged 5.710. Actual
size RPCs peaked at eight in both legs. Writer prepared-work waits were
60.028 seconds (86.4% of main) and 9.404 seconds (51.8%). Writer batch time was
7.759/6.952 seconds; it matters more after the forced path's read waits shrink.
Unforced encryption counters recorded 141.865 origin worker-seconds, 15.489
crypto queue worker-seconds and 82.466 crypto service worker-seconds. Forced
recorded 126.748, 234.734 and 64.672 respectively. These overlap across requests
and are not additive wall-time shares. Header-cache evictions were zero.

**Verdict: correctness accepted; amplification candidate not qualified.**
The descriptive main-time ratios are 2.984 and 2.615 versus the historical
control, but logical metadata work rose by 81.8% and 7.6%. This is evidence of
latency overlap, not a measured amplification reduction. The combined setting
change and historical, non-interleaved control cannot isolate causes. Do not
promote this bundle, raise defaults, or call its short screen sustained acceptance.

Evidence root:
`/volume2/NASDA2/vaultic-test-m6/metadata-read-fme-cae1-20261103-10k-coalesced`.
Its immutable `verified-comparison.json` distinguishes capture acceptance from
amplification, sustained and deployment qualification. All 11 launch pins and
38 root-relative `METADATA-READ-SHA256SUMS` entries verified. The earlier control
remains sealed and unchanged. Representative separate-variable cache/caller
screens, catalog loading cost and naturally ten-minute repeated full-crawl gates
remain open; correctness fixtures do not substitute for those measurements.

## Correctness Contract

1. Size/mtime and normally ctime/inode describe source changes, not repository
   blob availability. Keep checks for every required content reference.
2. Missing or malformed verified Vaultic metadata must take the ordinary safe
   read/rechunk path. A first post-import backup does not inherently require
   `--force`; that flag removes the parent and forces rereading.
3. Cache exact ID, type, found status and size. A probabilistic positive must
   never authorize content reuse. Generation/session identity and new-write
   overlays must preserve the existing freshness and deletion/retention contract.
4. Preserve authentication, encryption authentication, error identity, deadlines,
   caller cancellation, shared-request cancellation semantics and fail-closed
   daemon behavior. Do not turn lookup failure into a negative or positive hit.
5. Bound queues, handles, bytes, request lifetimes and retained directory state
   through the shared working-memory policy, not merely a goroutine count.
6. Preserve source ordering, hardlinks, directory edges, exclusions, ancestor
   coverage, snapshot scope, writer fencing and acknowledged durable completion.
7. A borrowed verifier connection must not close an owned daemon client. Test
   reader closure, owner `WriterStatus`, cleanup and encrypted reopen explicitly.
8. Do not modify production repositories, installed binaries/services, frozen
   seeds, sealed evidence or retention policy during diagnostic qualification.

## Options

| Option | Intended Reduction | Cost or Risk | Initial Decision |
| --- | --- | --- | --- |
| Fit decoded SST index/filter metadata in engine cache | Repeated fetch/decrypt/decode of hot metadata | Memory accounting; cache admission and invalidation | First configuration experiment after attribution |
| Cross-file batches plus SlateDB engine `multi_get` | Repeated table/index/filter visits and duplicate block reads | Bounded buffering, ordering, cancellation, engine parity | First code candidate if metrics show sharing opportunity |
| Bounded parallel unchanged-file validation/prefetch | Exposed synchronous storage latency | Does not necessarily reduce work; can increase contention and RSS | Combine only with measured work reduction or a separately reported latency-overlap hypothesis |
| Exact compact local blob catalog | Per-file DB requests after a sequential load | Startup work, memory/scratch use, generation/session freshness | Evaluate if lookup workload exceeds practical cache coverage |
| Local encrypted object/range cache | Repeated backend fetching | Decryption still required; quota and stale-version checks | Prefer over plaintext storage when backend fetching dominates |
| Trusted decrypted object/range cache | Fetching and repeated decryption | Explicit plaintext trust/permissions and approval | Separate security decision, not an implicit fallback |
| Reduce LSM/SST overlap or adjust compaction | Tables examined per point lookup | Writes, startup/compaction cost, maintenance authority | Only after overlap metrics demonstrate a cause |
| Move authoritative metadata to SSD | Access cost, not amplification itself | Deployment and durability change | Separate approved deployment experiment |

The existing `MultiGet` RPC can loop over independent engine gets. Reducing RPCs
does not necessarily share engine work: distinguish transport batching from the
`VAULTICDB_SLATEDB_MULTIGET` engine path. The flag is a candidate to test, not a
proven production recommendation.

Engine metadata caching is decoded in-process caching. Encrypted object caches
sit below decryption; trusted decrypted caches sit above it. Keep those layers
distinct in measurements and security decisions. An exact local catalog can
eliminate individual DB reads, but its initial scan remains part of startup and
total command cost; do not hide that cost outside the comparison.

## Execution Protocol for an LLM

Before each phase, read current applicable repository instructions and the
owning implementation and nearby tests. Preserve concurrent user edits. State
one falsifiable hypothesis, one focused disconfirming check, the smallest edit,
and the phase exit gate before editing. Validate immediately after the edit.
Do not implement multiple options before identifying which measurement supports
them. Do not launch a larger workload or change security/resource constraints
without approval.

For every experiment, retain a create-only manifest containing source revision
and diff, Go/Rust toolchains, actual executable paths and SHA-256, dependency
revision, input hash/count/types/bytes, parent/snapshot identity, cache and worker
settings, backend topology, source and seed provenance, owned process identities,
resource limits, commands, scheduling order, raw logs, observations and outcome.
Verify checksums from the manifest root. Preserve failed/preliminary captures.

Diagnostic envelope: private durable NFS evidence roots with exact 0700
directories and 0600 logs/configuration, short private `/run` sockets/scratch,
swap disabled, core dumps disabled, owned RSS ceiling no greater than
200,000,000,000 bytes, managed CLI RAM no greater than 160 GiB, CLI scratch budget
2 GiB, and an explicit shared free-space floor established before launch.
Fixture databases and packs are durable experiment storage, not hidden scratch;
declare their separate byte ceilings. Monitor the whole owned process group.
Use observed SIGINT cleanup before bounded kill fallback. Never force takeover
to conceal failed cleanup. Smaller phase-specific budgets are preferred.

### Phase 0: Seal the Baseline and Forced Control

Owner: `cmd/vaultic/working_benchmark_test.go` and existing qualification helpers.

Task: execute the requested complete 10,000-entry frozen `fme_cae1` comparison.
Establish one ordinary verified parent with all source data already cataloged;
close it, clone its DB/repository into separate legs, checksum-verify each clone,
and relocate only each private WAL binding. Hold the same source path/inodes,
content, binary, settings and publication group constant. Unforced uses the
explicit common parent; forced uses `--force` and must have no parent.

Check: exactly 10,000 observed/scanned entries; no failed/deferred entries;
matching regular-file count; zero newly stored content/data blobs in both legs;
report tree metadata and aggregate `data_added` separately rather than requiring
them to be zero; unforced regular files all unchanged; forced regular files reread rather
than counted unchanged; complete snapshot scope with no reused subtrees; exact
restore and source hash/type/size agreement; encrypted reopen; idle writer;
no owned survivors. Include setup, main, command and verification durations
separately. This is one diagnostic pair, not a statistically qualified gain.

Parent setup is explicit preconditioning: the 10k setup added two data blobs and
tree metadata from an older historical seed. These additions are outside the
measured legs, which both start with all source content present in the catalog.
Preserve setup summaries and costs rather than claiming zero setup additions.

Exit: two accepted leg results plus accepted outer cleanup, retained raw critical
frames/RPC/cache/native counters and independent checksum verification. On any
failure, retain the outcome and repair only the demonstrated local defect.

### Phase 1: Attribute the Amplified Reads

Owners: `internal/index/blob_lookup_cache.go`, daemon `client_wait.go`, native
`service/kv.rs`, `storage.rs`, `storage/cache.rs`, `encryption.rs`, and the pinned
SlateDB read/cache abstraction only where needed.

Hypothesis: repeated index/filter or rounded encrypted-range work dominates
metadata reads and survives warm passes because an effective cache is absent,
undersized or bypassed. This is a hypothesis, not an established root cause.

Task: extend fixed-cardinality metrics for requested plaintext bytes, rounded
ciphertext bytes, raw fetch duration, crypto queue/service duration, engine read
duration, cache hit/miss/admission/eviction and range class. Capture bounded
repeat-range samples using opaque identifiers; do not log secrets or unbounded
paths/blob IDs. Measure NFS traffic separately from logical wrapper bytes;
physical HDD attribution additionally needs permitted storage-side counters.
Explicitly mark unavailable counters rather than substituting zero.

Check: deterministic injected fetch/crypto/queue delays must be attributed to
the correct interval; boundaries must not overlap when reported as a partition.
Concurrent tests must preserve request outcomes and bounded state. Exercise
headers/capabilities through the actual authenticated transport, including old
daemon compatibility and cancellation/error cases.

Exit: reproducible attribution on the fixed 10k workload; choose one next option
from measured cache/range behavior. If raw fetching versus crypto remains
unresolved, refine this phase rather than guessing from NFS media type.

### Phase 2: Screen Cache and Engine Configuration

Hypothesis: measured hot decoded metadata can fit within an approved larger
cache, or genuine engine multi-get can share repeated range work.

Task: first compare metadata cache sizes 128, 256, 512 and 1024 MiB with block
cache, concurrency, source and binary fixed. Change one variable per leg.
Then compare engine multi-get off/on at fixed batches of 1, 8 and 64 handles,
including duplicates, mixed blob types, absent IDs and malformed records.
Count unique keys, table visits, required/coalesced reads and bytes; singleton
engine calls alone cannot demonstrate cross-file batching benefits.

Check: all results equal the control, memory accounting is bounded, total
startup/command cost is reported, and the targeted read work decreases. Reject
a purported amplification fix that only changes RPC latency or timing placement.

Exit: select a cache/configuration candidate only if measured work reduction
and memory cost support it. Otherwise advance to the specific code option
supported by Phase 1, not a combined speculative tuning bundle.

### Phase 3: Implement One Bounded Read-Work Candidate

Option A owners: archiver directory traversal and `allBlobsPresent`, cached blob
lookup, daemon read session, and nearby archiver/cache/session tests.

Task A: introduce bounded cross-file prefetch/coalescing where unchanged
validation currently blocks the directory-entry loop. Keep deterministic node
submission and per-file missing-content fallback. Account batch handles, result
buffers, pending deduplication and queue bytes before allocation. Maintain a
bounded flush/lifetime policy; cancellation must not strand callers or poison
unrelated shared requests. `ReadConcurrency` controls content readers and is
not automatically the unchanged-validation concurrency setting.

Option B owners: blob-lookup cache/catalog and read-session identity.

Task B: if an exact local catalog is selected, stream an ID/type/size view into
an admitted RAM or private scratch representation. Specify completeness,
generation/sequence pinning, negative-answer validity, duplicate handling and
new-write overlay semantics before coding. Count loading time and bytes. Stop
on budget exhaustion; never silently trust incomplete positive/negative state.

Checks for either option: unchanged, changed, missing blob, lookup failure,
malformed record, duplicate content, hardlink, symlink, excluded subtree,
renamed path, source mutation, cancellation while queued/in flight, daemon
loss, private writer closure and reopen. Use existing tests/helpers; add only
coverage that does not have a suitable existing home. Race-check shared paths.

Exit: focused correctness tests and the exact 10k control/candidate comparison
pass; the selected read-work metric decreases. No production activation.

### Phase 4: Find the Useful Concurrency Knee

Task: on the selected fixed implementation/cache setting, screen concurrency
1, 8, 16, 32, 64 and 128, stopping earlier on resource or tail-latency failure.
These values are an experiment grid, not proposed defaults. Use identical
input and retained, explicitly distinct odd/even execution schedules; do not
reverse a mirrored schedule and call it order alternation.

Specify whether each experiment changes the metadata caller pool or its cache
RPC limiter; change one at a time and log both effective values. Do not claim
that raising content `ReadConcurrency` changes unchanged-validation callers.
An apparent plateau behind a fixed four-RPC limiter is not an engine peak.
Attribute location-slot waiting explicitly before claiming the forced read
path is limited inside storage alone.

Then confirm the apparent knee with at least three paired sustained captures,
each main interval naturally lasting at least ten minutes on an approved
sufficiently large unchanged source. No sleeps, artificial throttling,
favorable-repeat replacement or short-screen extrapolation. If input is too
small, stop for approval of a larger fixture rather than quietly expanding scope.

Check: throughput, p50/p95/p99/max request latency, actual peak in-flight reads,
slot/queue waits, CPU, RSS, working budgets, range/cache work, backend traffic,
errors and source/snapshot identity. Define a practical plateau as less than
5% median throughput improvement for two successive concurrency increments
across repeated captures; this is a tuning criterion, not a universal peak.
Prefer the lower setting when throughput is comparable and tails/resources grow.

Exit: documented useful operating point for this implementation, source and
backend, including variance and rejected captures. Recheck if caching/batching
changes; the knee is not independent of amplification.

### Phase 5: Full-Crawl Qualification and Deployment Decision

Task: compare frozen control and candidate builds with the selected budgets on
approved representative full-source input. Retain ordinary unchanged and
forced controls separately; do not mix their bottleneck diagnoses. Establish
at least 100 entries/second over sustained completed unchanged main intervals,
without moving startup/catalog-load cost outside command reporting.

Check: complete coverage, exact restore/content/type/size, expected parent,
zero new data for unchanged input, changed/missing-content fallback, encrypted
reopen, cancellation/recovery and no resource/ownership/durability regression.
Report whether metadata logical work and actual backend traffic decreased,
and whether throughput improvement is batching, caching or overlap.

Exit: reviewed immutable acceptance receipt and an explicit production
deployment approval. Retain a pinned rollback binary/configuration; never
remove availability checks or weaken crypto/durability to meet throughput.

## Required Phase Handoff

At each phase end, record: hypothesis; exact edit/configuration; source and
binary pins; focused test names/counts; raw evidence root and checksum result;
workload/settings; accepted and failed outcomes; performance and work deltas;
resource peak; cleanup; unresolved questions; and the one authorized next task.
Do not claim a full test passed when only its measurement passed. Do not commit
or deploy unrelated changes. Future agents must read the last accepted handoff
and current files before continuing.