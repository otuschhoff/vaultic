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

## Chunk Comparison Protocol (2026-11-04)

A create-only forced 200k comparison completed under
`/volume2/NASDA2/vaultic-test-m6/chunk-compare-fme-cae1-20261104-200k`.
All 16 legs passed correctness and cleanup and were sealed. Do not treat the
historical 916.574 entries/second result as a paired baseline for this feature.

The fixed variants are:

| Variant | Native source | Retained authenticated chunks |
| --- | --- | --- |
| A | `01835ca98b1b66d48f176b719fffa8f89ac80c67` | Original contiguous read behavior |
| B | `931e3a5ef29c632fa483e304abe7b3d1a842865b` | Zero bytes; concurrent-fetch coalescing remains active |
| C | Same as B | Default 64 MiB |
| D | Same as B | 256 MiB; only retained-byte capacity changes |

Both native revisions were freshly compiled from isolated committed source
archives using identical locked release builds and the same toolchain. Build
logs must identify the actual archive source path, and the resulting hashes must
differ. One newly built CLI and qualifier are shared across all variants. The
CLI includes a launcher fix that explicitly forwards the six chunk-limit
variables; without it, a zero-retention environment override was filtered out.
Focused race tests cover forwarding through the real command builder and
preservation of the ambient-secret filter.

The primary schedules are `A B C C B A` and `B C A A C B`, followed by the
capacity bracket `C D D C`. Each leg uses the same accepted retry2 frozen input
and independently checksum-clones its untouched parent. The source remains
200,000 entries, 178,208 regular files and 23,520,039,140 bytes, with digest
`c1f7faee11872b1fac2f0af5d242c7a30f51ddc426bfd3e5c93b7200b3eefc83`.
No leg inherits another leg's process caches, repository writes or daemon.
Normal identical startup work may warm caches before the measured main phase.
Host/NAS caches are not dropped or controlled; the balanced ordering exposes
some drift but does not eliminate environmental uncertainty.

Reader concurrency 128, RPC limit 32, publication group 16, metadata cache
2 GiB, block cache 512 MiB, eight crypto workers and 1 ms batch delay stay fixed.
All new variants explicitly use the original entry, flight-count, reserved-byte,
chunk-size and range-count defaults. Live owned-daemon environment and crypto
thread checks must confirm the selected settings, not merely launcher intent.

Every leg retains main and command duration, CLI CPU, process CPU/RSS samples,
cache and encrypted-read deltas, and reconciliation/publication attribution.
Writer-status deltas cover backup through snapshot validation and are wider
than the main-phase timer; compare like windows. Native cache occupancy and
in-flight gauges are not exported by the Go receipt and remain unavailable to
this comparison. Sampled process CPU is not an exact main-phase CPU integral.
Origin bytes are logical object-store payload, not physical NAS traffic.

B versus A measures the net coalescing-path effect, including fresh metadata
validation overhead. C versus B isolates retention. C versus A answers the
overall throughput question. The report retains individual legs and balanced
block means; its conservative support flag requires both blocks' combined
throughput gains to exceed the observed baseline elapsed-time spread. This is
not formal statistical qualification. The final capacity bracket is a separate
small experiment, not evidence for raising defaults or other limits.

Correctness requires full forced scope, zero added content blobs, exact restore,
encrypted reopen, unchanged source, idle writers and owned cleanup. Distinct
private mount namespaces protect production, the source, accepted fixtures and
workspace read-only in each child. Existing RSS, disk-space, fixture-size,
timeout and SIGINT-first cleanup guards remain in force. Failures stop the
series and preserve evidence; existing roots and seals are never overwritten.

Only an actual single main interval of at least 600 seconds satisfies the
sustained-duration gate. Repeated short 200k passes, cloning, hashing and restores
must not be added together to claim it. No workload expansion, production
replacement, host-cache flush, NFS tuning, unforced optimization or deployment
is part of this comparison.

### Observed Results

| Variant | Runs | Median main seconds | Median entries/s | Mean logical origin GB | Mean cache hits | Max combined RSS GB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| A: old native | 4 | 280.236 | 718.9 | 39.980 | 0 | 40.700 |
| B: coalescing, no retention | 4 | 289.837 | 690.1 | 39.762 | 0 | 40.769 |
| C: 64 MiB retention | 6 | 268.354 | 745.3 | 29.459 | 40,090 | 41.142 |
| D: 256 MiB retention | 2 | 199.059 | 1,008.3 | 28.973 | 41,930 | 41.631 |

Times above are per-leg main-phase medians, not totals or sustained ten-minute
measurements. The means for the two balanced six-leg blocks tell a more useful
story than a cross-series median:

| Block | A mean seconds | B mean seconds | C mean seconds | C versus A throughput |
| --- | ---: | ---: | ---: | ---: |
| `A B C C B A` | 289.011 | 303.932 | 306.695 | -5.77% |
| `B C A A C B` | 280.236 | 268.571 | 266.224 | +5.26% |

The A controls themselves ranged from 246.776 to 331.247 seconds, a 34.23%
max/min elapsed spread. Even the sign of the measured default-cache improvement
changes between balanced blocks. This series **does not establish a causal
200k throughput improvement**, despite a repeatable reduction in logical
payload reads. Do not promote C to a qualified throughput default.

B joined approximately 834-852 concurrent chunk loads per leg but saved only
about 0.218 GB of logical origin payload versus A, approximately 0.54%. B added
approximately 146,700 header validations per leg; their accumulated timings
were 189-225 worker-seconds. C had about 40,090 retained hits per leg, reduced
mean logical origin from 39.762 to 29.459 GB versus B (25.9%), and still made
approximately 146,700 metadata validations per leg. C evicted approximately
108,000 chunks per leg. These are aggregate counter windows and cannot be
subtracted from elapsed time or interpreted as physical NFS bytes or exclusive
latencies. The workload demonstrably exercises retention, but storage/service
variability and validation cost can absorb the work reduction.

The `C D D C` capacity bracket yielded C/D means of 228.546/199.059 seconds,
or an apparent 14.81% larger-cache throughput advantage. Yet C's two bracketing
times were 267.737 and 189.356 seconds as the overall series sped up. D saved
only an additional 0.486 GB of logical origin work versus C (1.65%) with about
1,840 more hits. The timing cannot establish that increasing capacity caused
the 14.81% difference. Keep 64 MiB as the default; 256 MiB is an opt-in
experiment, not a deployment choice based on this bracket.

Every leg retained an exact 200k forced backup, unchanged-source digest, zero
new content blobs, full restore, encrypted reopen, verified idle writer and
accepted owned cleanup. All 16 per-leg manifests independently verified (432
files), as did the series manifest (51 entries) and all 12 launch pins. Private
leg runtimes were removed and no owned qualifier/native process survived. The
largest combined RSS sample was 41,631,424,512 bytes, below the 200 GB limit.
The unrelated pre-existing profile daemon was not part of this series. The
fastest main interval was 187.140 seconds and the slowest 331.247 seconds: **no
single run met the 600-second sustained gate**. The report's support flag is
false; formal causal, sustained and production qualification remain open.

Further causal throughput inference requires a fresh, separately pinned repeated
comparison with environmental I/O variability controlled or measured, not a
selective rerun of favorable legs. Measure metadata validation overhead and
actual host/NAS read behavior in matching phase windows before selecting cache
capacity or changing defaults. A new sustained-duration workload requires
separate scope and resource approval; do not extend the 200k duration by adding
clones or restores.

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

### Forced DB Resource Screen

The next forced-only screen holds content readers at 64 and metadata RPCs at 32,
except for one explicitly isolated 64-RPC probe. It uses the same frozen 50k
source, verified parent and measurement-pinned CLI/native binaries. No production
or NFS configuration changes are permitted.

| Profile | Crypto Workers | Metadata / Block Cache | Batch Delay | RPC Limit |
| --- | ---: | --- | ---: | ---: |
| control | 8 | 128 MiB / 512 MiB | 1ms | 32 |
| crypto32 | 32 | 128 MiB / 512 MiB | 1ms | 32 |
| meta2g | 8 | 2 GiB / 512 MiB | 1ms | 32 |
| block8g | 8 | 128 MiB / 8 GiB | 1ms | 32 |
| batch5ms | 8 | 128 MiB / 512 MiB | 5ms | 32 |
| rpc64 | 8 | 128 MiB / 512 MiB | 1ms | 64 |
| control-end | 8 | 128 MiB / 512 MiB | 1ms | 32 |

The beginning/end controls bracket run-order drift; the sequence is not a
replacement for repeated mirrored comparisons. Every non-control changes one
variable. The crypto setting is explicit rather than inferred from CPU count.
The authenticated launcher forwards it, and the supervisor requires observed
crypto-thread count plus cache/crypto environment from the owned native process.
Only those nonsecret settings are retained; credentials are not logged.

Each cell runs one forced full crawl from an independent checksum-verified
clone, then verifies exact restore, encrypted reopen, unchanged source, idle
writer and zero new data blobs. Existing private/read-only, 200 GB RSS,
350 GiB fixture, disk-floor, timeout and SIGINT-first cleanup guards remain.
Evidence roots append `-forced-db-{profile}` to the accepted 50k root.
Status: completed; all seven correctness captures and live-setting checks passed.
The analyzer sealed 27 files for the start control and 25 for each other cell;
all 177 checksums independently verified from their respective roots. There were
no candidate daemon survivors. Peak combined owned RSS stayed at 40.178--42.460
GB, well below the unchanged 200 GB ceiling.

| Profile | Main / Command Seconds | Entries/Second | Logical Metadata GB | Paired Storage Mean ms |
| --- | ---: | ---: | ---: | ---: |
| control | 74.573 / 93.896 | 670.484 | 451.079 | 39.491 |
| crypto32 | 78.982 / 100.099 | 633.057 | 458.435 | 36.490 |
| meta2g | 49.630 / 70.142 | 1007.446 | 1.132 | 1.772 |
| block8g | 74.450 / 92.569 | 671.592 | 452.873 | 38.889 |
| batch5ms | 85.599 / 105.013 | 584.117 | 439.509 | 40.177 |
| rpc64 | 76.941 / 96.727 | 649.847 | 451.629 | 39.343 |
| control-end | 76.589 / 97.261 | 652.838 | 463.024 | 40.666 |

The end control was 2.6% slower in throughput than the start control. The 2 GiB
metadata cache exceeded both controls by 50.3--54.3%, reduced main time by about
one third, and reduced logical metadata work by 99.75%. Native CPU fell from
1300--1344 seconds to 86.81 seconds; crypto queue/service work fell from about
1004--1061 / 261--262 worker-seconds to 1.621 / 6.688. This is strong evidence that
repeated hot metadata fetch/decrypt work controlled the read cost at 128 MiB.
`SlateDbTuning::cache` installs separate Foyer metadata/block caches through
`SplitCache`, so enlarging the 8 GiB block cache does not enlarge this metadata
cache. Direct metadata hit/eviction counters were not available in attribution;
the minimum cache capacity that fits this working set remains unmeasured.

The other screens distinguish symptoms from controlling causes. Crypto32 cut
queue time to 57.350 worker-seconds but increased service work to 435.449 and
native CPU to 1778.62 seconds; total throughput was 3.0--5.6% below controls.
Queue elimination alone was not a win. Batch5ms increased handles/RPC from
5.524--5.767 to 15.888 but reduced throughput 10.5--12.9%; fewer calls did not
justify the added wait. Block8g remained within the control range. Rpc64 peaked
at only 27 active size RPCs, with negligible admission wait and no throughput
gain. Do not combine these losing knobs into a presumed faster configuration.

For the next forced-only experiments, retain 64 readers / 32 RPCs, 8 crypto
workers, 2 GiB metadata / 512 MiB block cache and 1ms batching. This is a tested
diagnostic setting, not a changed default or production approval. At meta2g,
writer publication was 28.968 seconds (58.4% of main) and prepared-work waiting
14.780 seconds (29.8%). Publication batching is now the largest measured stage;
confirm the cache result and then vary publication group size independently.
None of these main intervals reached ten minutes or establishes repeated
sustained full-crawl qualification.

The immutable control `verified-db-screen.json` and each cell's
`DB-SCREEN-SHA256SUMS` retain complete measurements and live-setting evidence.
Do not rerun the create-only analyzer or overwrite sealed receipts.

Compare main/command throughput, metadata bytes/entry, batch density, RPC peak
and admission wait, paired storage latency, crypto queue/service, origin work,
daemon CPU and publication time. A faster result with unchanged work identifies
useful overlap; less work suggests batching/cache reuse, but does not establish
physical NFS savings. Cache-served origin work, overlapping worker-second sums
and unavailable full-SST decode attribution retain their existing limitations.
Neither resource headroom nor one short capture qualifies production defaults.
Unforced optimization remains deferred.

### Completed Forced Follow-Up Screen and 200k Scaling

The 23-cell forced-only follow-up completed with exact source, restore, encrypted
reopen, idle-writer, live-setting and cleanup acceptance in every cell. All 598
root-relative checksums and launch pins were independently verified before
changing the diagnostic driver. The create-only series evidence is under
`/volume2/NASDA2/vaultic-test-m6/forced-followup-fme-cae1-20261103`; each capture
has `verified-followup.json` and `FOLLOWUP-SHA256SUMS`.

Direct SlateDB cache counters are now available through the existing recorder:
`slatedb.db_cache.access_count`, with entry-kind and hit/miss labels. Concurrent
cache fetches retain SlateDB's own hit classification; errors are not misses.
These are cache-access counters, not physical NFS traffic or eviction counts.

| Metadata Cache | Entries/s | Metadata Hits | Metadata Misses |
| --- | ---: | ---: | ---: |
| 128 MiB | 640.4 | 1,661,516 | 188,758 |
| 256 MiB | 709.9 | 1,681,119 | 159,753 |
| 512 MiB | 818.4 | 1,732,727 | 83,413 |
| 1 GiB | 887.0 | 1,876,641 | 13,934 |
| 2 GiB, start | 975.3 | 1,926,836 | 327 |
| 4 GiB | 655.9 | 1,931,128 | 327 |
| 2 GiB, end | 754.1 | 1,941,228 | 327 |

The cache-fit boundary is between the tested 1 and 2 GiB settings for this
fixture. Two and four GiB had the same 327 metadata misses; 4 GiB showed no
additional work reduction. Throughput is not a causal cache-size ranking:
identical 2 GiB controls drifted from 51.268 to 66.305 main seconds.

Group-64 controls drifted from 75.093 to 51.349 seconds, while group-16 and
group-32 took 53.073 and 68.479 seconds. The automatic screen selected group 16
against the averaged controls, but it did not beat the ending group-64 control.
Do not call group 16 a qualified improvement. Hot-cache crypto and batch cases
took 50.629--53.304 seconds, while their identical controls took 57.717 and
49.732 seconds; these small apparent gains do not establish a winning setting.

The fixed-group-16 reader screen was more stable: 64-reader controls took
51.893 and 52.489 seconds; 96 and 128 readers took 50.562 and 49.003 seconds
(988.9 and 1,020.3 entries/s). The 128-reader result is a provisional scaling
candidate, not a repeated causal or production qualification.

Publication histogram export also has a population limitation: every forced
cell reported planning Count=0 and commit Count=5, totaling only
0.311--0.410 seconds. It does not account for the whole writer publication
stage; do not interpret it as proof that publication planning is free.

The user authorized a direct jump to 200,000 entries, explicitly skipping a
100k run. The guarded scale workflow freezes a new verified source subset,
creates a complete nonselective parent, then independently clones it for one
forced full traversal. Candidate settings are 128 readers, 32 RPCs, group 16,
2 GiB metadata cache, 512 MiB block cache, 8 crypto workers and 1ms batching.
The existing 200 GB RSS, 350 GiB fixture, 64 GiB source-byte and free-space
guards remain in force. The first attempt froze 200,000 entries and restored
23,520,039,140 source bytes, then failed a harness assertion that mistook its
source-freeze manifest for a bounded backup selection. No accepted parent
receipt or forced 200k measurement was produced. Its failed evidence is
preserved under
`/volume2/NASDA2/vaultic-test-m6/forced-scale-fme-cae1-20261103-200k`, with 23
artifacts verified by `FAILED-SHA256SUMS`.

The saved correction distinguishes a full frozen-source parent from a bounded
selection; all six crawl-mode regression cases pass under the Go race detector.
The end-to-end retry required fresh create-only paths and a newly pinned driver.
Source freeze, parent setup and restores do not count
toward the measured main interval. No short passes will be combined to claim
ten-minute sustained qualification. Production, defaults and NFS configuration
remain unchanged; unforced work stays deferred.

#### Accepted 200k Retry

The November 4 `retry1` froze the same verified source but failed before daemon
startup because its runtime name lacked the required `vaultic-unchanged-`
prefix. Its 21-file failed seal is preserved. The Python routing preflight now
rejects an incompatible runtime prefix or a runtime outside `/run`; Go scale
tests reject both failed capture roots. No safety guard was relaxed.

The fresh `retry2` completed forced-only acceptance with 200,000 entries,
178,208 regular files and 23,520,039,140 source bytes. Its measured main interval
was **218.203942960 seconds**, or **916.573721 entries/s**; the complete command
interval was 236.071219291 seconds. Exact restore, encrypted reopen, unchanged
source, zero added content blobs, live native settings and owned cleanup passed.
Peak combined RSS was 44,279,136,256 bytes, below the 200 GB ceiling.

Evidence is under
`/volume2/NASDA2/vaultic-test-m6/forced-scale-fme-cae1-20261104-200k-retry2`.
All 37 `FOLLOWUP-SHA256SUMS` entries and every launch pin were independently
verified after completion; the private runtime was removed and no owned
processes remained. The separate corrected driver has SHA-256
`4c8cd0339494f6a7ca4076c80f85248bb93223ee7573669742cd4b0fd5fb1f3c`.
Measured CLI/native binaries and the 128-reader, 32-RPC, group-16, 2 GiB metadata
cache, 512 MiB block cache, 8-crypto-worker, 1ms settings remained unchanged.

Metadata-cache misses stayed low: index 166, filter 157, stats 0. Block-cache
misses were 164,779. Encryption attribution reported 147,783 origin calls and
39,987,969,753 logical ciphertext-origin bytes for 1,482,193,190 requested
plaintext bytes, a 26.98x ratio. Origin calls accumulated 1,376.273 worker-seconds
(9.313ms per call), compared with 22.917 crypto-service and 3.128 crypto-queue
worker-seconds. The writer reported 107.148 seconds waiting for prepared work.
These overlapping counters must not be added as wall time; logical origin bytes
are not measured physical NFS traffic. They point toward investigating encrypted
range/block fetch amplification rather than increasing crypto workers or metadata
cache capacity, without establishing a causal optimization result.

The rate is 10.2% below the earlier 50k candidate, an unpaired scaling comparison
rather than a regression attribution. Publication histograms still cover only
five commits (0.429 seconds) and zero planning samples, not the whole publication
stage. The 218-second main interval does **not** satisfy the 600-second sustained
gate. Repeated causal and deployment qualification remain open; no 600k run,
production change or default activation is implied.

### Bounded Authenticated Chunk Reuse

The encrypted object store now coalesces overlapping range reads at authenticated
chunk granularity and retains complete authenticated plaintext chunks in a
byte-bounded LRU. A cancelled caller does not cancel a load needed by other
callers. Errors are shared, never retained as cache entries, and failed or
panicking loads release admission so later reads can retry.

As of November 9, authenticated-chunk retention defaults to 16 GiB and permits
up to 64 GiB. Other fetch/range defaults remain unchanged. Byte settings accept
exact decimal byte counts, not unit suffixes. Earlier qualification sections
retain their historical settings and do not describe the current defaults.

| Environment Variable | Default | Maximum |
| --- | ---: | ---: |
| `VAULTICDB_CHUNK_CACHE_BYTES` | 17,179,869,184 (16 GiB) | 68,719,476,736 (64 GiB) |
| `VAULTICDB_CHUNK_CACHE_ENTRIES` | 65,536 | 262,144 |
| `VAULTICDB_CHUNK_FETCH_LIMIT` | 64 | 256 |
| `VAULTICDB_CHUNK_FETCH_BYTES` | 16,777,216 (16 MiB) | 67,108,864 (64 MiB) |
| `VAULTICDB_CHUNK_MAX_BYTES` | 1,048,576 (1 MiB) | 4,194,304 (4 MiB) |
| `VAULTICDB_CHUNK_RANGE_LIMIT` | 4 | 16 |

Zero cache bytes disables retention while preserving concurrent-fetch
coalescing; other settings must be positive. The fetch-byte reservation must be
at least the configured maximum chunk bytes. Invalid settings fail startup.
The retention budget is shared by backing-store wrappers derived from one
encryption template, with separate identities for each backing store. Both
entry and byte budgets apply. Ciphertext, transient buffers and cache
bookkeeping are additional bounded overhead, not included in retained bytes.

By default the chunk path handles ranges spanning at most four chunks, with
each chunk at most 1 MiB; the eligibility settings above permit experiments
without changing the on-disk encryption chunk size. Full-object and larger
reads retain the contiguous fetch path rather
than allocating a flight per chunk. No speculative next-chunk read-ahead is
performed. Returned slices own their requested bytes and do not pin entire
cached chunks. Retained plaintext is zeroized when its final owner releases it.

Cache hits require current object metadata validation, matching object identity
and encryption header, and an available read key. Conditional/versioned reads
retain their object-store preconditions. Identityless objects bypass chunk
reuse; payload metadata changes fail closed. Writes/deletes invalidate retained
chunks, and read-key retirement purges plaintext for retired keys and prevents
older flights from retaining it afterward. The additional HEAD/stat validation
is deliberate freshness work; it must be measured independently of payload I/O.
Concurrent ordinary reads of the same cached object identity now share a
bounded, cancellation-safe in-flight HEAD validation. Conditional, versioned
and extension-bearing requests still validate independently, and sequential
reads still revalidate; neither local writer ownership nor ciphertext
authentication proves the object at a mutable path has not changed elsewhere.
Failed validations are not cached. This follow-up has focused native correctness
coverage but no new 200k throughput measurement; the sealed comparison above
predates validation coalescing.

Tagged compacted SST reads now use a narrower freshness policy: an unconditional
read with the same SlateDB tag (including segment), a stable object identity and
a header fetched or validated within the last second may reuse that header
without another HEAD. An expired header must be revalidated; a changed object
invalidates the retained chunks. Untagged/WAL, conditional and versioned reads
continue to validate on each cached-header use. A SlateDB validation-retry tag
invalidates local entries and refetches instead of replaying cached bytes. This
bounded policy does not provide immediate detection of an external deletion
during the one-second window; local writes and deletes invalidate immediately.
It has native correctness coverage but no post-change throughput measurement.

#### Backend Access and SST Freshness

Two independent startup controls are forwarded by the Go daemon launcher:

| Environment Variable | Values | Default |
| --- | --- | --- |
| `VAULTICDB_BACKEND_ACCESS_MODE` | `shared`, `dedicated`, `dedicated-force` | `shared` |
| `VAULTICDB_SST_FRESHNESS` | `strict`, `bounded`, `immutable` | `bounded` |

Freshness applies to encrypted object-store reads. `strict` validates every
cached header use; simultaneous requests with identical semantics can share
that validation. `bounded` permits reuse for one second. `immutable` permits
reuse for the lifetime of a retained header, not a time-to-live. Reuse requires
an unconditional, unversioned compacted-SST request containing only the known
SlateDB tag, an exactly matching tag (including segment), and a stable ETag or
version. Unknown extensions bypass header reuse, validation coalescing and
payload caching. WAL SSTs, manifests, leases and other mutable objects do not
gain permanent freshness exemptions. Retry-tagged reads discard local entries
and fetch from the origin instead of reusing payloads.

The immutable contract comes from the pinned SlateDB implementation, not writer
ownership: compacted SST IDs are unique, active manifests and reader checkpoints
retain referenced SSTs, and compaction publishes new SSTs instead of replacing
existing ones. This makes `immutable` useful in shared mode too. It is not a
physical-existence check: a fully cached object may still be returned after an
out-of-band deletion until eviction or invalidation. Do not select it when raw
tools replace SSTs in place or require immediate deletion visibility. Local
mutations invalidate before and after completion; a generation guard prevents
older in-flight header observations from repopulating the cache afterwards.
Eviction and process restart require a new origin observation. Authentication,
key availability and payload/object-identity checks are retained in every mode.

`shared` local backends take shared lifetime locks; `dedicated` takes an exclusive
lifetime lock and fails on conflicts rather than silently reverting to shared.
The sidecar is outside the deletable object namespace and uses the canonical
root, so reset does not delete it and symlink aliases cannot bypass it. The lock
is retained while the object-store wrapper is alive, including after database
close until references are dropped. Never delete or replace a live lock sidecar.
Local backend/cache/WAL directories require access to their lock sidecars even
in shared mode. Failed opens release their acquired guards.

Normal `dedicated` mode accepts only external, single-local-backend topology
on supported Linux local filesystems (ext, XFS, btrfs, tmpfs, overlayfs, ZFS),
with inherited WAL and no separate WAL handoff. NFS and other remote filesystems,
S3/RADOS/replicated stores, capsule topology and separate WAL targets are rejected
by that mode; they do not have an equivalent distributed access fence.

`dedicated-force` explicitly overrides the filesystem and backend-type checks
for NFS, other filesystems and remote object stores. It is an operator assertion
of sole access, not verification or a distributed fence. Local/NFS backends still
acquire exclusive advisory locks: force does not ignore a conflicting lock or an
unavailable locking service. Remote backends have no backend access lock; the
operator must exclude all other readers, writers, maintenance tools and hosts
using deployment controls and credentials. Existing writer-epoch fencing remains
active, but is not a sole-reader/writer access guarantee. Capsule topology,
separate WAL targets and WAL handoffs remain rejected even with the override.
The startup `backend_access_configured` event reports `mode=dedicated-force`,
`operator_asserted=true` and the locking mechanism (or `no-backend-access-lock`).
Neither dedicated mode automatically relaxes mutable metadata freshness or
changes the independently selected SST policy.

These are advisory locks among updated cooperating VaulticDB processes, not a
security fence against arbitrary programs, older daemons or raw administrative
tools. Stop older daemons before migration, restrict backend credentials and
filesystem access to the participating deployment, and keep maintenance tools
offline while dedicated mode is active. The existing writer epoch still governs
writer ownership in shared mode. Startup lifecycle events
`backend_access_configured` and `sst_freshness_configured` report the acquired
access mode and encrypted-read policy; invalid values reject startup.

For example, an encrypted local deployment can explicitly select both:

```sh
export VAULTICDB_BACKEND_ACCESS_MODE=dedicated
export VAULTICDB_SST_FRESHNESS=immutable
```

For an NFS or remote deployment whose sole access is enforced operationally:

```sh
export VAULTICDB_BACKEND_ACCESS_MODE=dedicated-force
```

Stop other clients and maintenance jobs before enabling this override; it does
not acquire or verify remote exclusivity for you.

This changes neither production configuration nor benchmark evidence. Cold,
random SlateDB block misses remain expected. No writer batching, preparation
queue or concurrency defaults are changed: prepared-work waiting can include
batch collection and upstream filesystem/content/metadata work, so it is not
evidence that the writer itself needs more concurrency. A new controlled screen
is required to establish throughput impact.

Read attribution adds `chunk_cache_hits`, `chunk_cache_misses`,
`chunk_cache_evictions`, `chunk_coalesced_reads`, `header_validation_calls` and
`header_validation_ns`, plus `header_validation_coalesced_reads` for joined
metadata checks. These monotonic counters pass through Go writer status
and reset-checked backup interval deltas; older daemons may omit them. Native
snapshots additionally expose current residency and in-flight gauges, which are
not treated as monotonic interval counters.

Validation covers 38 encryption tests, including persisted encrypted SST reads,
bounded admission, cancellation, shared authentication failures, replacement
races, key retirement, conditional reads and backing-store isolation. A
concurrent/nearby-range regression reduced payload fetches from 33 to one. The
warm persisted-SST fixture performed zero SST payload reads with SlateDB's block
cache disabled, while still performing metadata validations. Native storage
attribution and an authenticated encrypted backup/restore fixture also passed.
These are correctness and fixture results, not a new 200k performance screen or
production deployment qualification.

### Reconciliation Diagnostics and 400k Comparisons

The completed scale is explicitly 400,000 entries, with the same immutable SST and
dedicated-force benchmark policies. Historical 200k fixtures remain sealed and
unchanged. The new source is frozen under
`/volume2/NASDA2/vaultic-test-m6/reconcile-parent-fme-cae1-20261105-400k`;
comparison evidence uses the separate sibling
`reconcile-read-fme-cae1-20261105-400k`. The 64 GiB logical-source ceiling,
200 GB combined-RSS ceiling, private ownership checks, disk reserve, read-only
mount isolation, SIGINT-first cleanup, inherited-WAL conversion receipts and
restore/reopen correctness requirements remain enforced.

Publication groups now prefetch current pointers and then their immutable
revision values in two bounded MultiGet rounds. Missing records preserve the
old behavior; failed or malformed grouped responses fall back to scalar reads.
The cache is group-scoped and does not retain current pointers between groups.
Pointer, revision and content-manifest RPC calls/keys/time are reported
separately. Comparison time excludes content retrieval and debt-resolution RPC
waits. Existing aggregate timers retain their historical definitions and must
not be added to the new stage timers.

Collection reports first-item wait, batch-fill wait, ready-batch blocking and
sampled prepared-queue occupancy (empty, 1--25%, 25--50%, 50--75%, 75--100%).
JSON backups emit `reconciliation_progress` every second alongside the existing
30-second full critical-path report. Producer first/last timestamps and observed,
scanned and queue counters permit windowed production/consumption analysis.
With read-ahead enabled, collection waits are collector worker time and overlap
coordinator work; `writer_wait_prepared_ns` remains coordinator phase time.

The following backup controls support isolated comparisons:

| Flag | Default | Scope |
| --- | --- | --- |
| `--metadata-reconciliation-scalar-revision-reads` | false | Diagnostic control disabling revision prefetch only |
| `--metadata-reconciliation-batch-size` | 0 = 5000 | Collection size, maximum 10000 |
| `--metadata-reconciliation-read-ahead` | 0 | 1 collects the next batch while the coordinator processes the current batch |

Read-ahead is opt-in. Database lookups and publication remain ordered in one
coordinator. Storage is bounded to an active batch, one queued batch and one
batch being collected, in addition to the configured existing item queues.
Hardlink, directory and conflicting-inode handling stays with the coordinator.

Authenticated-chunk diagnostics sample 1/64 of chunk identities into a bounded
16,384-entry hash history. They count successful sampled first/untracked fetches,
re-fetches, history evictions, elapsed reuse time and intervening fetched bytes,
with cumulative 64 MiB / 256 MiB / 1 GiB volume thresholds. No paths, keys or
plaintext are logged. These are sampled estimates, not exact global unique-chunk
counts. History eviction can make a revisit appear first/untracked, and fetched
volume is not LRU stack distance or a counterfactual cache-hit prediction.
Uncacheable range paths are outside the identity sample.

Read-volume windows separately retain requested SST plaintext and origin
ciphertext in up to 360 ten-second buckets. All range paths and header origin
reads contribute to these volume windows. Go transport validates window fields,
ordering, retention and payload size. Window receipts subtract the writer-before
snapshot through main-end; boundary buckets are partial and this interval is
not identical to the exact main-phase counter delta. Dropped windows are
explicit. An origin-only bucket has unknown amplification, not infinity or zero.

The 400k runner is selected with `--mode=coalesced --inode-limit=400000
--concurrency-profile=db-scale400k`. Run preparation using `--prepare`, stage the
pinned qualifier binary, and execute the parent in a private mount namespace.
The explicit `--reconcile-series` selection then uses the same pinned CLI/native
binaries for all twelve legs:

- J/K/K/J: scalar versus batched revisions.
- K/L/L/K: collection read-ahead off versus on.
- K/M/M/K: collection batches of 5000 versus 500.

Each bracket has two observations per variant and changes one control only.
J/K/L/M share the 64 MiB authenticated cache, 128 readers, RPC cap 32, group 16,
2 GiB metadata cache, 512 MiB block cache, eight crypto workers and 1ms batching.
Neither cache capacity nor encryption encoding is changed speculatively.
Reports retain observed ratios, all correctness gates and raw evidence; they do
not claim statistically established causal speedups or sustained qualification
unless the corresponding evidence exists. No production configuration changes.

#### Accepted 400k Results

The parent and all twelve comparison legs passed exact entry-count, forced
backup, zero new data blobs, restore, encrypted reopen and unchanged-source
checks. The frozen source contains 355,389 regular files and 31,813,367,320
payload bytes; its SHA-256 is
`0f650dea9f6cd4b768fdb1e27e1e959b73b07adc88ce8c84a551f3fcb2067819`.
Restore accounting includes ancestor directories and reports 355,571 files.
Parent preparation took 15,413.768 seconds including verification; its forced
main took 362.518 seconds. The parent used shared/bounded policies and is not
an immutable/dedicated-force performance control.

Each row below is one independent ABBA bracket. Times are exact main-phase
seconds rounded for display, not fixture or command durations. Observed
throughput ratios divide control median time by candidate median time.

| Comparison | Control Repetitions / Median (s) | Candidate Repetitions / Median (s) | Observed Throughput Ratio |
| --- | ---: | ---: | ---: |
| J vs K: revision batching | 487.285, 454.801 / 471.043 | 368.705, 329.700 / 349.203 | 1.349 |
| K vs L: collection read-ahead | 370.328, 301.713 / 336.021 | 267.755, 325.668 / 296.711 | 1.132 |
| K vs M: collection batch 500 | 332.849, 339.154 / 336.002 | 338.505, 209.330 / 273.918 | 1.227 |

Revision batching reduced revision lookup calls from 400,000 to 66,691--66,692
in the J/K bracket, about 83.3% fewer calls for the same 400,000 keys. Revision
lookup time fell from 188.689--195.959 to 53.684--57.332 seconds. Writer batch
time fell from 211.374--220.218 to 71.388--75.424 seconds. Pointer lookup time
remained 51.207--55.266 seconds. These counters include directory/scalar paths,
not only regular-file publication groups; stage timers overlap and are not
additive wall-time partitions. Keep the implemented grouped revision reads and
their scalar error fallback.

After batching, sequential collection spent 203.603--271.198 seconds filling
batches across the six K legs, versus only 0.013--0.256 seconds waiting for a
first item. This is not evidence that stat service alone is slow: producer and
prepared-send blocking coexist with the coordinator collecting and processing
large batches. With L, coordinator prepared-work wait was 135.030--201.839
seconds, while collector fill time was 245.603--303.898 worker-seconds and
overlapped processing. Producer/prepared-send blocking was zero in both L
captures, but pointer/revision lookup times increased. The bounded overlap
changes pipeline scheduling rather than reducing encrypted read volume.

M has substantial variance: one run was slightly slower than its neighboring
K controls, while the other took only 209.330 seconds. The fast M run had
*higher* pointer/revision and writer batch times than the slower M run, but
coordinator wait fell from 211.186 to 53.454 seconds. Its origin worker time
also differed by roughly tenfold despite similar byte volume. Two observations
cannot establish that the smaller collection batch caused the median gain.
Keep read-ahead and batch 500 opt-in; retain collection defaults of 0 and 5000.

Exact main-phase origin ciphertext was 49.602--49.693 GB for approximately
1.720 GB of requested plaintext: 28.840--28.893x logical amplification. All
twelve main windows recorded zero header validations. Neither revision batching
nor these collection controls materially reduced the amplified byte volume.
Broader writer-before/validation windows must not be substituted for these main
counter deltas. Ten-second volume receipts are phase-dependent: lookup-heavy
windows often reach roughly 55--65x, with dips followed by renewed high ratios,
not monotonic cache warming. Boundary buckets are partial and do not isolate
individual lookup stages.

Each main capture sampled 1,297--1,400 first fetches and 1,481--1,724 re-fetches,
with no sampled-history evictions. Genuine repeated authenticated fetches are
therefore present, not merely cold first visits. Mean sampled intervening fetch
volume was 10.813--11.753 GB; only 0--2 re-fetches per leg were within 64 MiB,
27--94 within 256 MiB, and 108--258 within 1 GiB. These cumulative sampled
volume thresholds are not exact LRU stack distances: they do not establish a
cache-size counterfactual or justify blindly raising the cache. Locality and
encrypted-read granularity remain more targeted investigation candidates.

Peak combined owned RSS across comparison legs was 40,782,082,048 bytes, below
the unchanged 200 GB ceiling. Independent root-relative verification passed
336 leg checksums, 39 series checksums and all 11 launch pins; the parent had
36 independently verified seals. No owned executable survivors remained and
all twelve private comparison runtimes were removed. Accepted receipts remain
sealed under `reconcile-read-fme-cae1-20261105-400k/verified-series.json` and
each leg's `verified-followup.json` and `group-64/forced/result.json`.

The ratios are descriptive results from two observations per side, with host
cache and service variability uncontrolled. `causal_speedup_claim` remains
false. All main intervals were below 600 seconds, so sustained qualification
is not established. Physical NFS traffic was not measured, and deployment is
not qualified. No cache/concurrency defaults, production settings, NFS tuning
or cache flushing were changed by this comparison.

### Isolated 400k Cache-Capacity Screen

The follow-up screen tests retention rather than increasing lookup concurrency
or changing collection scheduling. Representative accepted 400k receipts show
zero CLI blob-size lookup evictions (213,113 peak entries, 349,525 capacity),
about 2.9 million index/filter hits each with only 158/169 misses, and about
261,000 engine data-block misses. These wider receipt counters prioritize data
blocks and authenticated chunks over increasing lookup or metadata capacity.

Fresh evidence uses
`/volume2/NASDA2/vaultic-test-m6/cache-read-fme-cae1-20261107-400k-retry4`, with pinned
executables under `bin/phase35-cache400k-retry4-guard600/linux-amd64`. Historical parents,
rejected retry1 captures and the frozen source remain unchanged. The explicit runner selection is
`--mode=coalesced --inode-limit=400000 --concurrency-profile=db-scale400k
--cache-series`. No earlier create-only evidence root is reused.

| Variant | Engine Data Blocks | Authenticated Chunks | Chunk Entries |
| --- | ---: | ---: | ---: |
| N: control | 512 MiB | 64 MiB | 16384 |
| O | 2 GiB | 64 MiB | 16384 |
| P | 8 GiB | 64 MiB | 16384 |
| Q | 512 MiB | 512 MiB | 16384 |
| R | 512 MiB | 2 GiB | 16384 |
| S | 512 MiB | 4 GiB | 16384 |

The schedule is `NOON | NPPN | NQQN | NRRN | NSSN`: independent ABBA brackets,
two observations per side, one capacity variable changed per bracket. All
variants retain the same 2 GiB metadata cache, 128 readers, RPC cap 32, group
16, eight crypto workers, 1ms batching, batched revisions, collection size 5000,
read-ahead off and immutable/dedicated-force policies. Chunk entries are fixed
at 16,384 even for the control to isolate byte capacity; 4 GiB of 256 KiB chunks
requires that many entries. Flight limits and encryption chunk size do not
change. Existing hard caps suffice and production defaults remain unchanged.

Reports compare main time, exact-main origin bytes and chunk misses, wider-window
engine block misses, and peak combined RSS. Keep the two counter scopes separate.
Long fetched-byte reuse distance is not distinct-byte LRU stack distance, so
neither benefit nor failure at a particular capacity is assumed in advance.
Combine candidates only after separate evidence shows reduced work; do not
promote a cache default based solely on a favorable timing median.

The first attempt, without the `-retry1` suffix, stopped during the first leg's
fixture preparation and has no accepted measurement or completion receipt.
Its last logs precede the current host boot; the exact termination cause was
not recorded. All 11 launch pins verified on recovery, with no owned executable
survivors or private runtime remaining. Its partial clone is preserved, not
resumed or promoted to an accepted fixture. The fresh retry uses a separate
root, runtime prefix and bundle. Unrelated workspace changes are excluded from
the retry qualifier by building an isolated committed tree plus the benchmark
harness patch; runtime CLI/native binaries remain the qualified versions.

The retry was stopped gracefully during leg five after a workload mismatch was
identified. Four completed legs passed source/restore/reopen/zero-new-data
correctness, but all reported `reused=0`, `changed=400000` and zero revision
lookups, rather than the intended complete record reuse. Their main intervals
were 6915.278, 6931.966, 6984.981 and 6930.353 seconds (about 57--58 entries/s).
Writer batch processing took roughly 4513--4581 seconds, with substantial
fresh directory publication and revision allocation. These are fresh-metadata
publication captures, not evidence for the intended reuse-cache workload.

All 112 completed-leg checksums and 11 launch pins independently verified.
The supervisor exited on SIGINT with no owned executable survivors; the fifth
leg is explicitly canceled. Original receipts are preserved without rewriting
their correctness classifications. The cache acceptance guard now additionally
requires all observed entries to be reconciled and reused, zero changed/failed/
deferred entries and publication/allocation calls, and complete revision lookup
coverage. Regression tests reject each of the four mismatched captures.

Source identity remains under investigation: reconciliation keys use numeric
source `DeviceID`, and the source currently reports device 1048681 after the
host restart. The frozen content receipt did not record a historical device
identity, so identity drift is a hypothesis, not yet a proven cause. A verified
reuse preflight and suitable current-identity parent are required before another
long screen; do not remap identities or modify the historical parent blindly.

The corrected parent is staged separately at
`/volume2/NASDA2/vaultic-test-m6/cache-parent-fme-cae1-20261107-400k` from the
verified post-restart first N snapshot. Preparation verifies all 48 stopped
diagnostic seals and the completed leg seals, checksum-compares cloned database
and repository payloads, preserves inherited WAL topology, and records snapshot
provenance. It retains the original frozen source path and inode manifest;
neither filesystem IDs nor historical records are rewritten.

Before timing, the authenticated clone connection validates snapshot format
and checks 64 evenly spaced current pointers and their revision availability.
Every parsed revision key must match the live source device, inode and node
kind. Normalized snapshots require positive commit sequence and root revision;
their root FSID and inode can be zero or differ from the source identity.
Legacy-tree snapshots require zero normalized scope fields. Neither format's
root IDs establish source filesystem identity. Historical snapshot zeros do
not prove device drift.
This untimed sample warming
is identical across cache variants and excluded from the writer-before counters.
An incremental bounded JSON monitor stops a measured leg at its first changed,
failed, deferred, allocation or publication counter. Accepted captures still
require complete 400k reuse and revision lookup coverage, restore, reopen and
source checks. The performance target is at least 1000 entries/s in the exact
main interval (400k entries in at most 400 seconds); report correctness and
performance acceptance separately and inspect each candidate replicate.

The cache-screen supervisor enforces a separate 600-second backup-command
deadline using its monotonic clock and atomic launcher start/finish receipts.
Cloning, parent preparation, authenticated reuse preflight, restore and reopen
are excluded. A hung or over-budget backup rejects the leg and sends SIGINT
to the entire owned process group, including Vaultic and VaulticDB. Existing
bounded shutdown allows up to 120 seconds before SIGKILL fallback; shutdown
time is separate from the execution budget. Monitoring has 0.25-second polling
resolution, and completion after 600 seconds is rejected independently by the
launcher. At 400k entries, this is a 50 percent time margin over the 1000
entries/s target and corresponds to about 667 entries/s, not 100k ops/s.

The corrected parent completed full checksum verification. The first guarded
retry2 launch exited before daemon startup because forced-run receipts omit
the `files` field expected in initial-parent receipts. Source digest, bytes,
restore and reopen flags matched. That rejected startup and its pinned sources
are preserved under retry2 with `STARTUP-SHA256SUMS`; no backup was measured.
The cache-parent reader now derives only that absent count from the sealed
source receipt after checking exact entries, regular-file count and unchanged
source status. Python acceptance additionally checks digest and byte bindings.
Explicitly inconsistent counts are rejected, and the sealed parent is not
rewritten. The corrected run used a fresh retry3 root and qualifier bundle.

Retry3 stopped at native preflight after 1296.56 seconds of preparation, before
starting a backup or its runtime timer. Both snapshot root identities were
zero, and the preflight wrongly treated those zeros as source identities.
The initial attribution to legacy-tree format was incorrect: the next native
probe decoded a normalized snapshot with zero root IDs. The rejected clone, logs, source
archives and `PREFLIGHT-SHA256SUMS` diagnostic manifest are preserved; no reuse
or throughput result was accepted. The corrected check uses authenticated
current-pointer revision keys and validates each format's actual scope contract.
Go decoder round-trip and Python regression tests cover normalized zero or
different root IDs, legacy records and malformed scopes/source identities.
The first database-only probe stopped before sampling after 401.05 seconds;
its executable, source and diagnostic evidence are retained without a payload
seal. Before the next full fixture run, a bounded retry probe at
`/volume2/NASDA2/vaultic-test-m6/cache-native-probe-fme-cae1-20261107-400k-retry1`
tests the corrected preflight and scans current directory keys for source-root
identities. It uses a fresh checksum clone, retains evidence and does not copy
repository payloads, run a backup, or change the sealed parent.

The retry database-only probe passed all 64 authenticated source-pointer and
revision checks in 403.41 seconds, with 309739520 bytes peak combined RSS.
The normalized snapshot had zero root IDs, commit sequence 1324403 and root
revision 1324402; current-source references matched filesystem ID 1048681.
The directory scan completed but did not match stored source paths, so it does
not establish historical device drift. No backup was measured.

The first retry4 control leg reused all 400000 records with 400000 revision
lookups, zero changed records and zero allocation/publication calls. Its main
interval was 317.954 seconds (1258.04 entries/s); the backup command took
323.764 seconds under the 600-second guard. Restore, encrypted reopen and
unchanged-source checks passed. Peak combined RSS across the whole leg was
39927332864 bytes; this is not an exact-main-only RSS measurement.

The series initially rejected this valid capture because its report required
`reconciled == 400000`. The reconciler increments that counter only for new
publication, not reuse. The corrected gate requires observed/scanned/reused
400000 and reconciled/changed/allocation/publication zero. Updated tests and
full receipt replay passed. The original failure and pinned source are retained;
separate correction provenance and verified follow-up seals accept the completed
control without rerunning it. Remaining legs used fresh fixtures and separately
pinned continuation supervision with unchanged qualifier/CLI/native binaries.

All 20 legs completed the fixed `NOON | NPPN | NQQN | NRRN | NSSN` schedule.
Every leg exceeded 1000 entries/s: the observed range was 1248.90--1717.21
entries/s, with main intervals 232.936--320.282 seconds. Backup commands took
238.097--325.855 seconds, all below the 600-second limit; no timeout occurred.
Each leg reused all 400000 records with exact revision lookup coverage and
zero changed records, allocations or publication calls. Restore, encrypted
reopen, unchanged-source and zero-data checks passed throughout. Whole-leg peak
combined RSS ranged from 39.662 to 47.221 GB, below the 200 GB ceiling.

Each contrast below uses only its own four-leg bracket, with two observations
per side. Ratios are control median time divided by candidate median time;
logical origin reductions use exact-main authenticated ciphertext counters.

| Candidate | Control Main, s | Candidate Main, s | Observed Ratio | Logical Origin Reduction | Candidate Median Whole-leg Peak RSS, GB |
| --- | ---: | ---: | ---: | ---: | ---: |
| O: 2 GiB block / 64 MiB chunk | 286.849 | 244.673 | 1.172 | 0.31% | 40.241 |
| P: 8 GiB block / 64 MiB chunk | 296.961 | 261.318 | 1.136 | 0.32% | 40.562 |
| Q: 512 MiB block / 512 MiB chunk | 235.582 | 239.879 | 0.982 | 2.55% | 41.016 |
| R: 512 MiB block / 2 GiB chunk | 280.648 | 279.472 | 1.004 | 8.99% | 43.659 |
| S: 512 MiB block / 4 GiB chunk | 291.663 | 277.262 | 1.052 | 16.39% | 46.942 |

The 2 GiB block cache had the best observed paired time ratio, but origin
bytes fell only 0.31% and control times varied substantially. This screen
does not establish a causal speedup or an optimal production setting. The
8 GiB block cache did not justify its larger capacity over the smaller option
in these separate brackets. Larger chunk retention reduced logical reads,
but its time benefit was small or inconsistent; 4 GiB retention cost about
6.83 GB additional median whole-leg peak RSS against its paired control.
Do not change production defaults based on this short screen.

Status: the full cache screen and the 1000 entries/s target are accepted.
All 20 receipts were independently replayed, series/continuation/leg/bundle
seals verified from their intended roots, and original continuation source
pins verified before this documentation update. No owned executable processes
or runtime directories remained. The original reporting failure is preserved;
separate post-run validation records final acceptance. Replicated causal
speedup, sustained 600-second performance and physical NFS qualification are
not claimed.
All existing private ownership, exact
source/restore/reopen/zero-data checks, read-only child mount isolation, 64 GiB
source ceiling, 200 GB combined RSS ceiling, storage reserve and SIGINT-first
cleanup requirements remain. Main intervals shorter than 600 seconds remain
short screens; physical NFS and production qualification are separate gates.

### Required Backup Test Source

As of November 11, backup performance testcases must read the original
`/ncl1-1-vs-50/fme_cae1` mount directly. The user confirms this volume does not
change and represents future backups. Do not substitute a frozen, staged or
restored copy on NASDA2 or another filesystem without explicit permission.
NASDA2 may still hold disposable test repositories, database state and evidence.

The Python supervisor and Go force-comparison boundaries now reject substituted
source paths. An original-source comparison requires a newly qualified parent
whose real source path, filesystem ID and inode identities belong to the original
mount; do not relabel or translate the copied-source parent's identities.
The previous copied-source routes are historical and cannot be relaunched through
these guarded boundaries until their source and parent qualification are replaced.

The November 11 reader screen at `readers-read-fme-cae1-20261111-800k` was
interrupted with SIGINT during its first leg when this requirement was received.
It has zero accepted legs, no surviving owned processes, and no throughput result.
Its capture and remaining runtime directory are preserved, not accepted evidence.
Earlier NASDA2-copy measurements below remain historical; they are not results
from the original `ncl1` mount.

### Default Cache And 800k Bottleneck Diagnostics

The November 9 follow-up promotes 16 GiB/65536 entries to the native
authenticated-chunk cache default and raises the supported ceilings to
64 GiB/262144 entries. At the usual 256 KiB chunk size these entry budgets
match the byte budgets. Larger opt-in budgets must raise both settings:

```sh
VAULTICDB_CHUNK_CACHE_BYTES=68719476736
VAULTICDB_CHUNK_CACHE_ENTRIES=262144
```

Capacity is a ceiling, not an eager 16/64 GiB allocation. Accounting remains
shared across backing-store wrappers of one encryption template; independent
daemon processes have independent budgets. Authentication, freshness policy,
invalidation, crypto concurrency and in-flight fetch/range limits are unchanged.
The default change does not deploy or restart a production daemon. Existing
explicit environment overrides continue to win.

The supervisor defaults to 800000 entries and selects `db-scale800k` for
the default coalesced route. The historical create-only follow-up root was
`bottleneck-parent-fme-cae1-20261109-800k-retry3`, using the qualified parent
`bottleneck-parent-fme-cae1-20261109-800k-retry2` read-only and the bundle
`bin/phase35-bottleneck800k-20261109-retry3-final/linux-amd64`. The parent has
its own real frozen-source identities and successful full restore and encrypted
reopen receipts. Do not relabel, overwrite, or extend sealed parents. Tuning
is group 64, collection batch 5000, one-batch read-ahead, 128 readers, RPC limit
32, metadata 2 GiB, block 512 MiB, eight crypto workers, and explicit
16 GiB/65536 chunk retention. Historical routes retain their original settings.

The measured forced-backup deadline doubles from 600 to 1200 seconds. It
excludes source preparation, parent creation, restore and reopen. The whole-leg
ceiling doubles from 28800 to 57600 seconds and the Go qualifier timeout from
7h50m to 15h40m; individual setup command timeouts double from four to eight
hours. The 600-second minimum for a sustained main interval is unchanged.
Source size remains capped at 64 GiB, fixture size at 350 GiB, combined owned
RSS at 200000000000 bytes, and the established shared free-space floor remains
enforced. An 800k selection that cannot fit these guards fails closed; raising
the sample count is not authorization to expand unrelated resource ceilings.

New diagnostics separate:

- Origin object fetch service, including body consumption and failed requests.
- Caller waits for authenticated chunks, including coalesced followers and
   decryption, separately from waiting for fetch admission.
- Actual source reader calls, returned bytes, errors and time inside `Read`,
   excluding chunk-splitting and hash CPU.
- Existing source stat/preparation and writer queue waits.
- Shared per-mount NFS READ/metadata RPC counts, received protocol bytes,
   queue time, RTT and execution time alongside process CPU/I/O/pressure.

Read durations use eight bounded, non-cumulative buckets: at most 100 us,
1 ms, 5 ms, 20 ms, 100 ms, 500 ms, 2 seconds, and above 2 seconds. The Go
authenticated transport preserves the histograms and rejects malformed arrays.
Main-interval subtraction rejects resets, copies arrays without mutating
baselines, and leaves missing older-daemon histograms unavailable.

Accepted 800k captures require all three DB latency histograms, source-read
coverage, and private nonempty CPU profiles bounded to 32 MiB. The supervisor
emits `bottlenecks` and `bottleneck_windows` in the verified receipt. The latter
retains at most 2048 approximately aligned windows, explicit truncation counts,
scan rates and correlations with source/fetch latency, preparation waits and
shared NFS READ RTT. Constant or unavailable series have null correlations.
CPU profiles cover the whole backup command, not native CPU or exact-main CPU.

These measurements support choosing the next optimization, not declaring
NFS the dominant bottleneck in advance. Worker/caller timers overlap and must
not be added to estimate wall time. NFS mount counters are shared and measure
RPC traffic, not exclusive backup traffic or physical NAS disk reads. Temporal
correlation is not causal attribution. A larger live sample does not by itself
qualify retained-history growth. Prior 400k results below remain historical
evidence, separate from the accepted 800k measurement below.

### 800k Qualification Results

The accepted forced-only follow-up is retained at
`/volume2/NASDA2/vaultic-test-m6/bottleneck-parent-fme-cae1-20261109-800k-retry3`.
Independent analysis and matched CLI CPU profiles are sealed in the sibling
`bottleneck-parent-fme-cae1-20261109-800k-retry3-analysis` directory. Verification
checked all capture/bundle checksums, 19 build-source pins, 14 launch pins,
receipt replay, no surviving owned processes, and removal of the private runtime.

| Measurement | Result |
| --- | --- |
| Traversal | 800000 inodes; 677957 regular files; 54715539376 source bytes |
| Main interval | 392.191 seconds; 2039.82 inodes/second |
| Whole backup command | 398.336 seconds |
| Whole qualification including setup and verification | 7172.241 seconds |
| Peak combined owned RSS across qualification | 68669112320 bytes (63.95 GiB) |
| Reuse | 800000 reused; zero changed/failed/reconciled records |
| Publication | Zero revision allocations, directory publications or atomic groups; zero new data blobs |
| Integrity | Full selected-source restore, unchanged source, and encrypted reopen verified |

The CLI summary's 1470 added bytes are metadata, not new file-data blobs. Forced
traversal still reread all 54.72 GB of selected file contents. The CLI profile
covers the complete command: 1077.40 sampled CPU-seconds over 398.10 seconds,
not an exact-main or native CPU profile.

Remaining costs are mixed:

- Source reads: 2742821 calls, no errors, mean 0.778 ms; bucket upper bounds
   p50 0.1 ms, p95 5 ms, p99 20 ms. Across 11 usable aligned windows, source-read
   mean latency and scanned throughput had Pearson correlation -0.781.
- DB origin fetches: 152466 calls and 40.39 GB logical ciphertext; mean
   10.30 ms, p50 at most 5 ms, p95 100 ms, p99 500 ms. Chunk-result waits averaged
   11.80 ms. Fetch-admission waits were zero, so raising admission limits is not
   supported by this run. Origin latency/throughput correlation was -0.269 over
   13 windows; shared NFS READ RTT correlation was -0.107.
- Retention pressure remains: 206093 chunk hits, 150763 misses and 85282
   evictions with the 16 GiB/65536-entry limit. This prompted the paired
   16-versus-32 GiB screen below, not an assumed gain from raising the default.
- CLI CPU: chunk splitting accounts for 17.71% cumulatively, SHA-256's block
   implementation 13.63% flat, syscalls 20.46% flat, and path cleaning 12.13%
   cumulatively. Repeated normalization in reconciliation conflict checks was
   subsequently optimized as described below, without altering chunk/hash semantics.
   These CPU percentages are not
   wall-time savings or proof of host-wide CPU saturation.
- Pipeline reporting records 158.95 seconds waiting for prepared work and
   189.14 seconds in writer batches. Source/DB accumulated worker times overlap
   these intervals; do not add them. Write-side durability was not dominant in
   this reuse run: only 13 commit RPCs and about 1.11 accumulated durable-wait
   seconds, unlike fresh parent creation.

Only 13 approximately aligned windows are available. Correlations are descriptive,
not causal; source reads may hit OS/NAS caches, and no physical NAS disk traffic
or backup-exclusive NFS traffic was measured. One 392-second pass does not meet
the unchanged 600-second sustained-main gate. No retained-history-growth or
production-throughput claim follows from this single capture.

The investigation also repaired a fresh-publication blocker: retained Begin
reconciliation history filled its old 4096-entry bound. The bound is now 65536,
with amortized expiry scans and unchanged cancellation/ownership retention rules;
burst, hard-bound, expiry and serialization tests passed. The patched parent
backup completed in 7417.118 seconds, followed by full verified restore and
encrypted reopen. Earlier flag-rejection, ledger-exhaustion and storage-limit
attempts remain preserved as failures, not benchmark wins.

The storage-limit failure occurred while retaining a full parent restore plus
duplicate comparison copies. The follow-up borrows the parent input/database/
repository read-only and charges their 193492423421 bytes against the original
350 GiB active-fixture ceiling along with its writable copy. Full restore
coverage is retained by verifying and releasing one top-level subtree at a time;
the failed captures remain unchanged. The ceiling, shared free-space floor,
200 GB RSS guard and 1200-second measured-backup deadline were not raised.

### 800k 16 Versus 32 GiB Cache Results

The November 10 screen completed all eight legs at
`/volume2/NASDA2/vaultic-test-m6/ram32-read-fme-cae1-20261110-800k`, with sealed
independent verification in its sibling `ram32-read-fme-cae1-20261110-800k-analysis`.
The fixed schedule was `16/32/32/16 | 32/16/16/32`, four observations per capacity.
Both byte and entry budgets changed: 16 GiB/65536 entries versus 32 GiB/131072
entries. All other settings and optimized CLI/native bytes were identical.
Each leg started a fresh daemon; host caches were not dropped or controlled.
Ordinary backend policies were retained, without historical benchmark-only
`dedicated-force` or `immutable` policies.

| Leg | Cache, GiB | Main Interval, Seconds |
| --- | ---: | ---: |
| 1 | 16 | 399.770 |
| 2 | 32 | 405.355 |
| 3 | 32 | 402.362 |
| 4 | 16 | 396.602 |
| 5 | 32 | 391.485 |
| 6 | 16 | 396.373 |
| 7 | 16 | 398.739 |
| 8 | 32 | 394.440 |

| Median Measurement | 16 GiB | 32 GiB |
| --- | ---: | ---: |
| Main interval, seconds | 397.670 | 398.401 |
| Logical ciphertext origin bytes | 40389727676 | 28630409796 |
| Main-interval chunk evictions | 85276.5 | 0 |
| Peak combined owned RSS across qualification, GiB | 63.64 | 75.12 |
| Whole-command CLI CPU, seconds | 1012.986 | 1029.727 |
| Mean source-read latency, ms | 0.753 | 0.764 |
| Mean origin-fetch latency, ms | 11.234 | 15.332 |

32 GiB reduced logical origin reads by 29.11%, eliminated measured main-interval
evictions, and used 11.48 GiB more peak combined RSS. It did not consistently
improve throughput: its overall median elapsed time was 0.18% longer. The first
balanced bracket was 1.42% longer at 32 GiB; the second was 1.16% shorter.
Fewer origin requests do not imply lower mean latency among the remaining
requests, which can have a different composition. These observations do not
establish a causal slowdown or speedup.

All eight legs reused exactly 800000 records, with zero changed/failed/reconciled
records, revision allocations, directory publications, atomic groups or new data
blobs. Every leg passed full restore verification, unchanged-source checking and
encrypted reopen. Independent verification replayed every leg and the complete
comparison, checked capture/bundle seals and current source/artifact/parent pins,
confirmed actual native cache settings, and found no owned processes or private
runtimes remaining. The shared floor, active-fixture cap, read-only parent/source
guards, RSS limit and deadlines remained unchanged.

Before this screen, publication conflict checks were changed from pairwise
repeated normalization to bounded per-group identity/source/normalized-path
indexes. Identity and path conflict decisions, ordering and normalization rules
are unchanged. Reference-equivalence and race tests, the full reconciliation
suite and the actual encrypted native publication/reuse/reopen fixture passed.
A three-repeat 64-item microbenchmark fell from approximately 2.1 ms to 45 us
per group, about 46 times less CPU time, not a whole-backup speedup.
Matched profiles in this screen show conflict-set insertion at 3.72--4.34 sampled
CPU-seconds and reconciliation path normalization at 16.76--17.81 CPU-seconds;
the earlier unoptimized capture showed 88.30 seconds in pairwise conflict checks
and 101.55 seconds in normalization. This historical comparison is descriptive,
not an optimized/unoptimized paired throughput experiment.

Remaining costs are mixed. A representative 16 GiB profile attributes 18.96%
cumulative CPU to chunk splitting, 14.60% flat CPU to SHA-256's block function,
and 22.63% flat CPU to syscalls. Source reads still return all 54.72 GB per leg,
with no observed read errors. Writer waiting for prepared work ranges from
249.39 to 271.53 seconds, while writer-batch time ranges from 91.70 to 109.15
seconds. These accumulated intervals overlap other work and are not additive
wall-time attribution. Fetch-admission waits remain zero in all eight legs.

Keep the 16 GiB throughput default. 32 GiB is an optional logical-origin-load
reduction, not a demonstrated throughput improvement for this fixture. Before
raising concurrency, run a separate paired content-reader-concurrency screen
with cache and metadata concurrency held fixed, preserving source-read latency,
CPU and correctness checks. Retained-history/database-growth qualification is
still required before extrapolating either cache size to a larger database.

This source was the frozen NASDA2 copy, not the original `ncl1` source mount.
Logical origin bytes are not physical NAS disk traffic, shared NFS counters are
not backup-exclusive, and profiles cover whole CLI commands, not exact-main or
native CPU. No pass reached the unchanged 600-second sustained-main threshold.
No production configuration was changed. Documentation was updated only after
independent pin verification; its pre-update bytes remain in the pinned build
archive at `/run/vaultic-ram32800k-build-20261110/candidate`.

### Original-Source 800k Reader Results

The original-source reader screen launched November 11 completed all twelve
legs at
`/volume2/NASDA2/vaultic-test-m6/ncl1-readers-fme-cae1-20261111-800k-retry3`.
Every backup read `/ncl1-1-vs-50/fme_cae1` directly, using the same sealed
800000-entry bounded manifest. No NASDA2 input copy was substituted. The fresh
encrypted parent at `ncl1-parent-fme-cae1-20261111-800k-retry2` passed full restore,
encrypted reopen and unchanged-original-source verification before the series.
Its snapshot is
`bf1a0adba4175cda729572a6873ddae4695d944e21795f11587a2d44fc13a0a9`.

The fixed schedule was
`64,96,128,128,96,64 | 96,128,64,64,128,96`, four observations per setting,
with 128 readers as the control. Only content-reader concurrency varied.
Authenticated cache capacity stayed 16 GiB / 65536 entries; metadata RPC cap 32,
metadata cache 2 GiB, block cache 512 MiB, eight crypto workers, publication group
64, reconciliation batch 5000 and one-batch read-ahead stayed fixed. All legs
used the same CLI and native bytes and a fresh private daemon. Host caches were
not dropped; backend policies were unchanged.

| Leg | Readers | Main Seconds |
| --- | ---: | ---: |
| 01 | 64 | 379.552 |
| 02 | 96 | 836.526 |
| 03 | 128 | 387.120 |
| 04 | 128 | 381.104 |
| 05 | 96 | 401.383 |
| 06 | 64 | 373.430 |
| 07 | 96 | 385.030 |
| 08 | 128 | 416.519 |
| 09 | 64 | 400.976 |
| 10 | 64 | 351.147 |
| 11 | 128 | 346.937 |
| 12 | 96 | 368.228 |

| Median Measurement | 64 Readers | 96 Readers | 128 Readers |
| --- | ---: | ---: | ---: |
| Main seconds | 376.491 | 393.207 | 384.112 |
| Logical ciphertext origin bytes | 40575992204 | 40575861124 | 40575074644 |
| Main-interval chunk evictions | 86114 | 86113.5 | 86110.5 |
| Peak combined owned RSS, GiB | 63.69 | 63.88 | 63.54 |
| Whole-command CLI CPU, seconds | 921.651 | 930.013 | 903.234 |
| Mean source-read latency, ms | 0.369 | 0.386 | 0.368 |
| Mean origin-fetch latency, ms | 10.090 | 9.877 | 10.201 |

64 readers had a 1.98% shorter overall median main interval than 128, an observed
throughput ratio of 1.0202. Both balanced brackets had the same direction:
64/128 medians were 376.491/384.112 and 376.061/381.728 seconds, ratios 1.0202
and 1.0151. These small differences do not establish a causal or production
speedup. 96 readers did not consistently improve throughput: its bracket
medians were 618.954 and 376.629 seconds. The 836.526-second leg is retained,
not excluded or replaced with a favorable repeat.

That slow leg spent 703.814 accumulated seconds waiting for prepared work,
versus 204.301--284.229 in the other legs; its writer-batch and directory phases
were 100.448 and 29.958 seconds. Source-read mean latency was 0.544 ms and
origin-fetch mean latency 9.508 ms. Whole-command CLI CPU was 1030.927 seconds.
This localizes a symptom to preparation supply, not a proven physical NFS or
native-engine cause. Phase and worker timers overlap and cannot be added as
wall-time contributions. Matched CLI profiles remain mixed: representative
64/128 profiles show about 21.5% flat syscall CPU, 14.8% flat SHA-256 CPU and
18% cumulative chunk-splitting CPU. They are not exact-main or native CPU
profiles, nor evidence that all host CPUs were saturated.

All legs observed and scanned 800000 selected paths and reused all 799076
distinct metadata records. The original source contains 1228 selected hardlink
paths in 304 groups, hence 924 aliases; path count is not record count. Complete
before/after device/inode/type/link-count receipts and manifest hashes matched
across all legs. Revision lookups covered exactly those 799076 records, with
zero changed/failed/deferred/reconciled records, revision allocations, directory
publications, atomic groups or new data blobs. Full restore, source-stability
and encrypted-reopen verification passed for every leg.

The independent verifier checked the parent, bundle, series and twelve follow-up
seals; current source/artifact/parent pins; actual native settings; exact row and
comparison replay; all 24 matched profile exports; and absence of owned
processes and all thirteen active parent/leg runtimes. Its four-file sealed
report is beside the capture at
`ncl1-readers-fme-cae1-20261111-800k-retry3-analysis`.
Earlier failed preparations and the rejected reader-report attempt remain
separate historical evidence, not observations in this comparison. Necessary
compatibility fixes retained an explicit 800000-entry manifest maximum and
removed on-demand-only batching from dry-run preflight; hardlink-aware reporting
did not relax full path coverage or zero-publication checks.

Retain the 128-reader control and current cache settings; do not make a
production/default change on this small observed 64-reader advantage. 64 is a
candidate for subsequent representative qualification, not an established
winner. Logical origin bytes are not physical NAS disk traffic, shared NFS
counters are not backup-exclusive, and retained-history/database-growth behavior
is still unqualified. Eleven legs were shorter than the unchanged 600-second
sustained-main gate, so the complete screen does not qualify sustained
throughput. No production configuration was changed. Documentation was updated
only after independent verification; its pinned pre-update bytes remain in
`/run/vaultic-ncl1-readers800k-retry3-build-20261111/candidate`.

### Original-Source 800k RAM64 And Dependency Results

The November 12 screen completed all eight legs at
`/volume2/NASDA2/vaultic-test-m6/ncl1-ram64-fme-cae1-20261112-800k`.
Every backup read `/ncl1-1-vs-50/fme_cae1` directly, using the same sealed
800000-path manifest and qualified original-source parent from the reader
screen above. NASDA2 held disposable repositories, evidence and restore output,
not substituted input. The source device, root inode, selected path identities
and hardlink topology matched before and after every leg.

The fixed schedule was `16,64,64,16 | 64,16,16,64`, four observations per
capacity. Authenticated database-chunk cache budgets varied together:
16 GiB / 65536 entries versus 64 GiB / 262144 entries. Content readers stayed
128, metadata RPC cap 32, metadata cache 2 GiB, block cache 512 MiB, crypto
workers eight, publication group 64, reconciliation batch 5000 and read-ahead
one. All legs used identical instrumented CLI bytes, unchanged native bytes and
a fresh private daemon; host caches were not dropped. The supervisor had 56
available CPUs and no ancestor cgroup CPU quota. This is not evidence that CPU
work was on the critical path.

| Leg | Cache GiB | Main Seconds |
| --- | ---: | ---: |
| 01 | 16 | 364.195 |
| 02 | 64 | 368.239 |
| 03 | 64 | 356.444 |
| 04 | 16 | 363.954 |
| 05 | 64 | 365.474 |
| 06 | 16 | 374.324 |
| 07 | 16 | 403.844 |
| 08 | 64 | 384.731 |

| Median Metric | 16 GiB | 64 GiB |
| --- | ---: | ---: |
| Main seconds | 369.260 | 366.856 |
| Logical ciphertext origin bytes | 40576254364 | 28589512684 |
| Chunk evictions | 86115 | 0 |
| Peak combined RSS GiB | 63.788 | 74.880 |
| Whole-CLI CPU seconds | 861.319 | 895.071 |

The 64 GiB setting reduced median logical origin bytes by 29.541%, eliminated
recorded evictions, and increased median combined RSS by 11.092 GiB. Cache
misses were 105751 in every 64 GiB leg versus approximately 151474 at 16 GiB;
the bounded refetch sample contained no refetches at 64 GiB. Configured capacity
is not measured occupancy: this does not establish that a 64 GiB cache filled
or that 64 GiB is necessary. Median elapsed time was only 2.403 seconds
(0.651%) shorter, an observed throughput ratio of 1.00655, not a proven causal
or production speedup. Balanced four-leg bracket medians were 364.075 versus
362.341 seconds and 389.084 versus 375.103 seconds (16 versus 64 GiB), ratios
1.00478 and 1.03727. Both brackets favor 64 GiB, but the magnitude differs;
the slower seventh and eighth legs remain included without replacement.

New constant-space counters distinguish file admission, source reading,
read-inclusive chunk work, blob submission, callback completion, hashing and
metadata admission. These are accumulated overlapping worker/caller seconds,
not additive wall-time contributions. Main-interval median durations were:

| Dependency Duration, Worker/Caller Seconds | 16 GiB | 64 GiB |
| --- | ---: | ---: |
| File handoff to reader pool | 36.901 | 33.722 |
| File workers submitting blob work | 6782.521 | 5907.552 |
| Post-read wait for blob completion | 4881.422 | 3821.193 |
| Blob hashing | 123.234 | 125.381 |
| Blob metadata admission | 5935.152 | 4779.639 |
| Writer waiting for prepared work | 228.801 | 233.951 |
| Writer processing batches | 108.518 | 98.982 |
| Collector waiting to fill batches | 336.059 | 332.020 |

The measured dependency chain is file workers waiting to submit blob work,
bounded asynchronous blob tasks waiting for metadata admission, and file
completion waiting for those callbacks. `WithBlobUploader` limits its shared
group to `2 + GOMAXPROCS`; two long-lived tasks leave approximately 56 blob
slots here, held through hashing, admission and callback completion. All
811726 blob saves per leg were already known: admission is an existence/size
lookup, not new data publication. Its cached-lookup path can coalesce remote
size requests; the counters do not separately attribute coalescing, RPC-pool
wait and backend read time. Live windows reached 56 active admissions, but
sparse 30-second samples do not establish persistent saturation. Likewise,
the 128 file readers were not consistently busy in those samples. Buffered
future-result delivery does not support blaming that send for the stall.

Across all eight legs, writer-connection `MultiGet` attribution recorded
1035.709--1257.852 storage worker-seconds versus 84.287--91.106 seconds outside
the server handler. In the first leg, engine durable wait totaled only 0.436
worker-seconds. Database write durability is therefore not the large measured
wait in this unchanged/full-reuse workload; server storage time is not itself
physical disk time. Despite approximately 19.5% less accumulated blob-admission
time at 64 GiB, the prepared queue remained empty in approximately 96% of
recorded samples at both settings, with batch-ready handoff below 1 ms per leg.
Those queue samples are not a wall-time fraction. Writer starvation persisted:
less origin traffic did not translate into a comparable end-to-end gain.

The complete causal critical path remains unqualified. In particular, serial
directory traversal/previous-node discovery occurs before file handoff, and
this screen does not time its individual synchronous dependencies or establish
per-batch first/last-ready critical delays. Those are the next discriminating
measurements. A smaller/time-bounded reconciliation batch or an independently
controlled blob-task limit remains a hypothesis, not a justified default
change. Do not equate accumulated worker time, sampled CPU shares, cache
evictions or sparse gauge peaks with exclusive critical-path blocked time.

Every leg completed all 677957 regular-file lifecycles and drained every live
file/blob gauge. All 800000 selected paths were observed/scanned, with 799076
reused metadata records and 924 independently accounted hardlink aliases.
Changed/failed/deferred/reconciled records, revision allocations, directory
publications, atomic groups and new data blobs remained zero. Full selected
restore, unchanged-source and encrypted-reopen verification passed eight times.
The independent verifier checked parent/bundle/series/eight follow-up seals,
24 current source pins before this documentation edit, actual settings,
identity/preflight/manifest receipts, diagnostic coverage, exact report replay,
16 matched profile exports, zero owned survivors and all nine runtimes absent.
Its sealed analysis, including dependency windows, is at
`ncl1-ram64-fme-cae1-20261112-800k-analysis` beside the capture.

Validation included focused lifecycle/callback/error race checks, 30 supervisor
tests, seven observer tests, nine compiled qualifier checks, and broad archiver
and repository race suites. Broad testing was not initially clean: root DAC
bypass invalidated archiver permission fixtures, resolved by a scoped capability
drop for that suite. `TestReadCacheChunkBoundaryAndFinalShortRead` reproduced on
committed HEAD and was the only repository exclusion. Two initial candidate
cache failures did not reproduce in three fixed isolated repetitions per side
or subsequent full baseline/candidate suites; their failed logs remain sealed,
not relabeled as reproduced baseline failures. Native cache fixtures required
the qualified binary at their hardcoded crate debug path in private source
archives. No unrelated cache product fix or workspace fixture-link change was
made to obtain passing validation.

All eight main intervals were below the unchanged 600-second sustained-main
gate. This qualifies bounded backup correctness and the observed cache/read
comparison, not sustained throughput, larger-database/history growth, physical
NAS traffic or a production speedup. Retain the current production/default
settings; 64 GiB is an optional logical-origin-load reduction candidate, not an
established throughput winner. No production configuration was changed.
Documentation was edited only after independent verification; its pinned
pre-update bytes remain at
`/run/vaultic-ncl1-ram64800k-build-20261112/candidate`.

### Original-Source 800k Worker-Opening Results

The November 13 follow-up instrumented previous-node lookup, metadata-only
opening, content opening, metadata `Stat`, node conversion, directory work and
subtree loading, plus batch first/last readiness and prepared-item queue age.
Counters use constant space and main-interval deltas. Directory/node stages
can nest, and some work runs in workers; their durations are not additive
exclusive wall time. Batch handoff age ends before the delivery callback so
it does not include writer processing in the no-read-ahead case.

A preliminary batch-size screen stopped after one accepted leg and two actual
successful backup/restore/reopen receipts. Batch 5000 took 350.827 seconds;
batch 1000 took 355.811 seconds. A hardcoded expected batch-5000 report flag
rejected the second receipt. The corrected report passed both real receipt
replays, but the failed capture was neither relabeled nor completed by mixing
later runs. Its evidence remains at
`/volume2/NASDA2/vaultic-test-m6/ncl1-producer-fme-cae1-20261113-800k`, including
the failed second-leg runtime at
`/run/vaultic-unchanged-ncl1-producer800k-leg-02-b1000`. This is not an accepted
balanced batch-size comparison. The observations did identify 154.420 and
150.626 seconds in serial content opening, versus only 0.166 and 0.166 seconds
in previous-node lookup. Smaller batches reduced the maximum first/last span
from 14.364 to 4.806 seconds, while accumulated prepared-item queue age rose
from 5.236 to 677.059 seconds. These two observations do not establish a
batch-size speedup or slowdown.

The evidence-driven candidate adds default-off `--read-open-in-workers`.
Content opening moves into the existing bounded file-reader workers, without
adding an unbounded queue or independent goroutines. Both modes share the
context check, content open, post-open `Stat` and regular-file validation.
Preparation failures close the source and complete the file exactly once;
changed-type errors retain the source path. `worker_open_calls` distinguishes
the modes rather than inferring them from timing.

All eight fresh comparison legs were accepted at
`/volume2/NASDA2/vaultic-test-m6/ncl1-open-fme-cae1-20261113-800k`.
Every representative backup read `/ncl1-1-vs-50/fme_cae1` directly, using the
same qualified original-source parent and sealed 800000-path manifest.
Only the opening flag varied, in schedule `0,1,1,0 | 1,0,0,1`, where one means
worker opening. Content readers stayed 128, metadata RPC cap 32, authenticated
chunk cache 16 GiB / 65536 entries, metadata cache 2 GiB, block cache 512 MiB,
crypto workers eight, publication group 64, reconciliation batch 5000 and
read-ahead one. Both modes used identical candidate CLI and native bytes,
with a fresh daemon per leg and no host cache dropping. Live child-namespace
kernel checks confirmed that the workspace, original source, production
repository and entire borrowed parent were effectively read-only, while
the editor workspace remained writable.

| Leg | Worker Opening | Main Seconds |
| --- | ---: | ---: |
| 01 | 0 | 775.571 |
| 02 | 1 | 279.610 |
| 03 | 1 | 273.834 |
| 04 | 0 | 350.749 |
| 05 | 1 | 268.293 |
| 06 | 0 | 358.103 |
| 07 | 0 | 387.921 |
| 08 | 1 | 274.282 |

| Median Metric | Serial Opening | Worker Opening |
| --- | ---: | ---: |
| Main seconds | 373.012 | 274.058 |
| Logical ciphertext origin bytes | 40576276337 | 40573632764 |
| Peak combined RSS GiB | 63.840 | 63.962 |
| Whole-CLI CPU seconds | 890.994 | 890.526 |
| Accumulated content-open seconds | 160.843 | 251.282 |
| Writer waiting for prepared work, seconds | 230.256 | 121.307 |
| Writer processing batches, seconds | 110.299 | 119.217 |
| Collector waiting to fill batches, seconds | 339.020 | 240.158 |
| Blob hashing, worker seconds | 125.846 | 126.491 |
| Blob metadata admission, worker seconds | 5839.550 | 7600.559 |
| File handoff to reader pool, caller seconds | 34.050 | 70.188 |
| Post-read wait for blob completion, worker seconds | 4785.317 | 6265.630 |

Worker opening reduced observed median elapsed time by 26.529%, a throughput
ratio of 1.36107. Balanced bracket medians were 563.160 versus 276.722 seconds
and 373.012 versus 271.287 seconds, ratios 2.03511 and 1.37497. The slow first
control is retained, not replaced: it recorded 392.784 seconds opening content,
264.506 seconds in directory work and 640.324 seconds waiting for prepared
work. This does not identify a physical NFS or native cause. The second bracket
also favors worker opening without that observation; neither bracket is a
production or host-cache-controlled causal speedup qualification.

Accumulated content-open time increased because opens now overlap inside the
reader pool; that is not a regression in serial critical time. Writer supply
wait decreased, while blob-admission, file-handoff and post-read worker/caller
durations increased. The unchanged logical origin traffic and whole-CLI CPU
do not establish either storage or CPU as the sole remaining bottleneck.
Blob metadata admission remains a substantial measured dependency, but these
captures do not emit separate blob-lookup slot/result/RPC attribution. A
separately bounded blob-task experiment would need its own controls; no such
limit or cache/batch default was changed here.

Every leg completed all 677957 regular-file lifecycles, with zero pipeline
errors and all live file/blob gauges drained. Content-open calls were exactly
677957 in both modes; worker-open calls were zero in controls and 677957 in
candidates. All 800000 paths were observed/scanned, with 799076 reused metadata
records and 924 independently accounted hardlink aliases. Changed, failed,
deferred and reconciled records, revision allocations, directory publications,
atomic groups and new data blobs remained zero. Full selected restore,
unchanged-source and encrypted-reopen verification passed eight times.

The independent verifier checked parent/bundle/series/eight follow-up seals,
25 current build-source pins before this documentation edit, 38 series pins,
actual settings and opening coverage, source identities and manifests,
preflights, live diagnostic windows, exact report replay, 16 profile exports,
zero owned survivors and all new private runtimes absent. Its sealed
`dependencies.json`, `measurements.json`, `summary.json` and `verification.json`
are at `ncl1-open-fme-cae1-20261113-800k-analysis` beside the capture. The old
failed batch-screen runtime remains intentionally preserved, not silently
counted as cleaned. Whole-CLI profiles do not measure exact-main or native CPU.

Validation passed full touched archiver and reconciler race suites, actual
800000-limit manifest parsing, bounded backup/verified restore in both modes,
preparation failure/cancellation/type-change/completion checks, 33 supervisor
tests, seven observer tests and ten compiled qualifier policies. Archiver
permission fixtures used a scoped DAC capability drop; reconciler fixtures
explicitly selected the qualified native binary. Prior fixture/report failures
remain disclosed, and unchanged repository code retains the earlier cache-test
qualification caveats. No unrelated cache fix or new exclusion was introduced.

Seven of eight main intervals, including every candidate, were shorter than
the unchanged 600-second sustained-main gate. This is a bounded correctness
and observed opening-mode comparison, not sustained throughput, larger-history
growth, physical NAS traffic or production qualification. Keep worker opening
opt-in and production/default settings unchanged. Documentation was edited
only after independent verification; its pinned pre-update bytes remain at
`/run/vaultic-ncl1-open800k-build-20261113/candidate`.

### Original-Source 800k Lookup And Coalescing Results

The November 14 blob-task screen added main-interval lookup slot, result and
RPC attribution plus opt-in `--blob-save-concurrency`. Default zero retains
`GOMAXPROCS`; explicit limits are 1--256 and cannot change after uploader
startup. The bounded group retains its two long-lived tasks in addition to
the selected blob-task limit. Worker opening was enabled in both arms.
Only four of eight planned legs completed: 56/112/112/56 tasks took
274.903/339.235/319.801/293.438 seconds. The partial balanced medians were
284.170 versus 329.518 seconds, 15.958% longer at 112 tasks. All four sealed
follow-up receipts passed restore, reopen, unchanged-source and independent
report replay. Only three series rows were finalized; the fourth follow-up
completed without its series profiles/row. No final comparison or failure
record exists. The launcher termination cause remains unestablished; kernel
journal inspection supplied no evidence. The incomplete capture remains at
`/volume2/NASDA2/vaultic-test-m6/ncl1-blob-fme-cae1-20261114-800k`, unchanged
and not mixed with later runs. It is not an eight-leg qualification.

Retained timed `MultiGet` client work was mostly inside server storage, not
outside the handler: storage was 1169.852--1810.577 worker seconds versus
66.705--80.221 outside-handler worker seconds. Durability wait was only
0.481--0.521 worker seconds. These scopes include metadata requests other
than blob-size lookup; server storage is not isolated physical disk/NFS time.
The negative concurrency result did not justify increasing blob tasks.

The November 15 follow-up added constant-space group collection and follower
wait counters, preserving main-interval and observer-window deltas. Collection
covers the leader's timer/flush wait and group retirement; follower wait
includes collection and RPC completion. Caller, group, follower and RPC
denominators differ, and their accumulated durations overlap. They cannot be
summed or subtracted as exclusive wall-time components. No coalescer behavior
or product default changed. The launcher now journals/fsyncs phases and legs,
records handled SIGTERM failures, and bounds profile extraction to 120 seconds
with owned-group cleanup. This improves future interruption evidence but
does not explain the old interruption or make SIGKILL catchable.

All eight fresh coalescing legs completed at
`/volume2/NASDA2/vaultic-test-m6/ncl1-coalesce-fme-cae1-20261115-800k`.
Every backup read `/ncl1-1-vs-50/fme_cae1` directly with the qualified original
parent and exact 800000-path manifest. Only metadata batch/coalescing delay
varied: `1,5,5,1 | 5,1,1,5` ms. Both arms used identical CLI/native bytes,
worker opening, 56 blob tasks, 128 readers, RPC cap 32, chunk cache 16 GiB /
65536 entries, metadata cache 2 GiB, block cache 512 MiB, eight crypto workers,
publication group 64, reconciliation batch 5000 and read-ahead one. Fresh
daemons, uncontrolled host cache without dropping, private read-only source,
workspace, production and entire borrowed-parent protection, resource caps,
deadlines and the 600-second sustained-main gate stayed unchanged.

| Leg | Delay ms | Main Seconds |
| --- | ---: | ---: |
| 01 | 1 | 260.104 |
| 02 | 5 | 347.854 |
| 03 | 5 | 367.737 |
| 04 | 1 | 289.428 |
| 05 | 5 | 351.048 |
| 06 | 1 | 289.126 |
| 07 | 1 | 290.722 |
| 08 | 5 | 349.647 |

| Median Metric | 1 ms | 5 ms |
| --- | ---: | ---: |
| Main seconds | 289.277 | 350.347 |
| Logical ciphertext origin bytes | 40573894924 | 40576123284 |
| Peak combined RSS bytes | 68378886144 | 67662911488 |
| Whole-CLI CPU seconds | 947.286 | 909.959 |
| Size RPC count | 83851 | 22931 |
| Handles per size RPC | 4.958 | 18.125 |
| Mean size RPC ms | 13.169 | 20.267 |
| Mean group collection ms | 1.152 | 5.217 |
| Mean follower wait ms | 17.545 | 30.463 |
| Slot wait, worker seconds | 0.778 | 0.542 |
| Result wait, worker seconds | 7369.061 | 13445.780 |
| Size RPC duration, worker seconds | 1120.707 | 465.335 |
| Group collection, group seconds | 96.525 | 119.608 |
| Follower wait, follower seconds | 5799.027 | 11972.163 |
| Writer waiting for prepared work, seconds | 136.577 | 198.565 |
| Writer processing batches, seconds | 120.313 | 118.589 |

Five milliseconds increased observed median elapsed time by 21.111% despite
larger groups and fewer RPCs. Balanced bracket medians were 274.766/357.795
and 289.924/350.347 seconds for 1/5 ms. RPC worker time fell, but follower
latency, total result wait and writer supply wait grew. Fewer RPCs alone did
not improve this bounded workflow. Retain the 1 ms / 56-task comparison
control; neither increasing blob concurrency nor extending collection delay
is supported. Writer supply gaps and blob completion dependencies remain
measured costs; these overlapping counters do not isolate their causal wall
path. Optimizing backend service would require a separately controlled test,
not an inference of physical NFS saturation from server-storage duration.

Every leg completed all 677957 regular-file lifecycles and worker opens,
811726 logical lookups and 415619 size handles. All 800000 paths were
observed/scanned with 799076 reused metadata records and 924 hardlink aliases.
Errors, changed/deferred/reconciled records, revision allocations, directory
publications, atomic groups and new data blobs remained zero; live file/blob
gauges drained. Full selected restore, unchanged source and encrypted reopen
passed eight times. The independent verifier checked parent, build, series
and all eight follow-up seals, current source pins before report repair,
actual settings/delays/limits, source identities/manifests, live coalescer
windows, exact report replay, 16 profiles, launcher phases, no owned survivors
and all new runtimes absent. Its sealed results are beside the capture in
`ncl1-coalesce-fme-cae1-20261115-800k-analysis`.

The initial series report returned inside the bracket loop, retaining correct
overall medians but omitting both bracket summaries. After independent raw
receipt verification, the report-only indentation was repaired and explicit
bracket count/order/value regression assertions added. All 38 supervisor
tests passed, and all eight actual receipts replayed identically; only the
two missing summaries changed. Original capture and analysis seals were not
rewritten. The corrected report, patch, test log and verification are sealed
separately in `ncl1-coalesce-fme-cae1-20261115-800k-bracket-correction`.

The isolated bundle passed focused lookup and command race tests, actual
authenticated native lookup, both bounded opening-mode restores, 38 supervisor
tests, seven observer tests and eleven compiled qualifier policies. A local
route compile failure was corrected before the sealed build and retained as
a validation summary. No new full repository-cache qualification is claimed;
earlier caveats remain. All eight main intervals were below 600 seconds.
This is not sustained throughput, larger-history growth, causal production
speedup or physical NAS traffic qualification. Production and defaults remain
unchanged. No additional backup run was substituted for a retained result.

### Native Read-Path Attribution Follow-Up

The November 16 follow-up traced `Storage::multi_get` below the service's
inclusive storage timer. That path validates keys, obtains any transaction
and read guards, awaits SlateDB reads and builds the bounded response. New
`multi_get_database_read` and `multi_get_transaction_read` timing stages now
cover the read futures before response assembly, with attempts, outcomes,
elapsed time, histograms and drained active gauges. Nontransactional database
read timing includes the database read-lock wait; transactional read timing
starts after the separately measured transaction-map and slot-lock waits.
Scalar mode counts one read per key, while native multi-get mode counts one
read per batch. These counts must not be interpreted as identical RPC counts.

The stages respect the existing attribution enablement policy, use additive
protobuf fields and pass through the authenticated status snapshot to Go and
main-interval backup DB-stage deltas. Older daemons leave the new stages zero;
that means unavailable attribution, not proof of zero database cost. The
change adds measurement only, not a read/cache/concurrency/default change.

Existing encryption metrics already separate underlying origin fetch,
chunk completion, admission, crypto queueing and crypto service. A read-only
reanalysis of all four sealed 1 ms / 56-task controls from the November 15
eight-leg original-source series found these medians:

| Existing Main-Interval Metric | Median |
| --- | ---: |
| Origin request plus body collection, worker seconds | 1568.756 |
| Chunk completion wait, worker seconds | 1794.751 |
| Chunk admission wait, worker seconds | 0.000 |
| Crypto queue, job seconds | 3.494 |
| Crypto service, job seconds | 22.640 |
| Header validation, worker seconds | 38.829 |
| Origin calls | 152834 |
| Authenticated chunk cache hits | 251541 |
| Authenticated chunk cache misses | 151465 |

The origin timer covers the underlying object-store request and collection
of its response body, stopping before encrypted chunk decryption. Chunk wait
overlaps origin and crypto work, and these daemon-wide main-interval metrics
also include reads outside blob-size RPCs. They are not additive exclusive
wall time and cannot be subtracted from blob-lookup caller totals. The
measurements make origin-fetch/chunk completion the next diagnostic lead;
they do not support increasing crypto workers or blaming chunk admission
waiting for the observed dependency. They do not prove physical NFS
saturation, isolate transport versus filesystem/backend waiting, or qualify
a particular prefetch/cache change.

All original series/analysis/correction seals and the four control follow-up
seals were checked, and each actual receipt replayed identically before the
reanalysis was sealed at
`ncl1-coalesce-fme-cae1-20261115-800k-native-read-review-20261116` beside the
capture. No new representative backup was needed for these existing-counter
medians. Every reused representative backup read `/ncl1-1-vs-50/fme_cae1`
directly. The new native-read timers were not present in those captures and
are not retroactively claimed as representative measurements.

Focused native tests cover scalar/batched reads, transaction ordering,
missing values and response limits with exact read-counter coverage. Go
tests cover the wire mapping, absent legacy fields and backup-stage
forwarding. A race-enabled encrypted authenticated lookup fixture against
the explicitly rebuilt diagnostic native daemon confirms completed/drained
read counters, owner usability after borrowed-reader close and encrypted
reopen. An initial local Go test compile failure used the wrong timing-field
name and attempted to compare a slice-containing snapshot; both assertions
were repaired and the same focused test passed. No new production benchmark
or throughput improvement is claimed; production and defaults remain unchanged.

### Original-Source 800k Native-Read Diagnostic

The requested November 16 rerun used a freshly rebuilt, isolated release
native daemon, CLI and qualifier with the new read stages. The single fresh
`leg-01-native` read `/ncl1-1-vs-50/fme_cae1` directly, using the same qualified
parent and exact 800000-path manifest: 677957 regular files and 54715539376
source bytes. Worker opening, 128 readers, 32 metadata RPC slots, 56 blob
tasks, 1 ms coalescing, batch size 5000 and all cache settings stayed fixed.
Old capture roots and binary bundles were not repointed or overwritten.

| Diagnostic Metric | Observed |
| --- | ---: |
| Main backup seconds | 282.724 |
| Entries per second | 2829.611 |
| Whole backup-command seconds | 291.196 |
| Whole-CLI CPU seconds | 910.805 |
| Peak combined owned RSS bytes | 69006422016 |
| Database-read batches / mean milliseconds | 26176 / 3.824 |
| Transaction-read batches / mean milliseconds | 82469 / 13.536 |
| Database-read worker seconds | 100.084 |
| Transaction-read worker seconds | 1116.339 |
| MultiGet server-storage worker seconds | 1217.643 |
| MultiGet outside-handler worker seconds | 68.706 |
| Lookup result-wait worker seconds | 7564.850 |
| Lookup RPC-slot-wait worker seconds | 0.866 |
| Coalescer follower-wait seconds | 5928.199 |
| Writer prepared-work wait seconds | 132.544 |
| Writer batch-processing seconds | 116.827 |
| Origin request plus body collection, worker seconds | 1744.159 |
| Chunk completion wait, worker seconds | 1914.319 |
| Chunk admission wait, worker seconds | 0.000 |
| Crypto queue, job seconds | 3.500 |
| Crypto service, job seconds | 22.297 |
| Logical encrypted-origin bytes | 40573370604 |

The two native-read stages total 1216.423 worker seconds, close to the
1217.643 MultiGet storage worker seconds. This is consistent with the
measured storage service mostly awaiting the native read futures, rather
than response assembly. Transaction-map and slot-lock waits were only
0.013444 and 0.002327 worker seconds, respectively; durable wait was
0.457205 worker seconds. All 108645 MultiGet calls completed without RPC
failures, invalid server timings or active work. Both native read stages
reported only successes, with no failures, cancellations or timeouts.

Blob lookup remained a large completion dependency: 811726 logical calls,
82469 size RPCs and 415619 size handles, with mean size-RPC time 14.265 ms,
mean collection time 1.149 ms and mean follower wait 17.794 ms. Blob-task
admission accumulated 7678.373 worker seconds, while RPC-slot wait was small.
All 677957 file lifecycles and worker opens completed; all 811726 blobs were
already known. Source reads accumulated 988.368 worker seconds, with zero
errors. The writer still experienced prepared-work supply gaps as well as
substantial batch processing. Origin-fetch/chunk completion, not crypto
queueing, remains the strongest lower-layer diagnostic lead. This does not
justify increasing task limits or coalescing delay; prior screens were
negative for those changes.

Every integrity gate passed: selected restore, encrypted reopen, unchanged
source and source identities, all 800000 observed/scanned paths, 799076
reused records, zero changed/failed/deferred/reconciled records, zero new
data blobs and zero new inode/directory publication or revision allocation.
All live gauges drained, owned children exited and the private runtime was
absent. Source/fixture/RSS/free-space/deadline guards and the private
read-only mount namespace remained in force. Independent verification
checked build/parent/diagnostic/follow-up seals and source pins, replayed
the actual receipt identically and rechecked all four historical controls.

Evidence is sealed beneath
`/volume2/NASDA2/vaultic-test-m6/ncl1-native-read-fme-cae1-20261116-800k`
and its `-analysis` sibling. The rebuilt native SHA-256 is
`fa7114f3043069229c1f0431a4c05bc56781db9871252c7c22dcf2ca16406317`;
the bundle is `bin/phase35-ncl1-native-read800k-20261116/linux-amd64`.
Release validation passed four native MultiGet tests, twelve timing tests,
fourteen focused Go race tests, twelve compiled qualifier policies/integration
cases, 39 supervisor tests and seven observer tests.

These are overlapping worker/caller/group/RPC metrics, not additive exclusive
wall time. Native DB stages cover all daemon clients; encryption metrics
are not exclusive blob-size RPCs, and origin bytes are not measured physical
NAS traffic. The whole-CLI CPU profile is neither exact-main nor native CPU.
The historical 1 ms control median was 289.277 seconds, but one rerun with
new native bytes does not establish a speedup or instrumentation overhead.
The main interval remains below the unchanged 600-second sustained gate.
No sustained-throughput, physical-NFS-saturation or deployment qualification
is claimed. Production and defaults remain unchanged.

### Origin Request And Body Attribution

The continuation after commit `861017ad2` splits the existing inclusive
origin timer into `ciphertext_origin_request_ns` around the underlying
`get_opts` future and `ciphertext_origin_body_ns` around response `bytes()`
collection. `ciphertext_origin_body_calls` counts attempted body collections;
requests that fail or fail identity validation before collection do not
increment it. Header reads, authenticated chunk fetches and uncached range
reads use the same boundaries. Existing total timing, histograms, identity
checks, decryption, caching and concurrency behavior remain unchanged.

The phases pass through the typed Go status snapshot and main-interval
subtraction with counter-reset rejection. Legacy snapshots without all three
fields remain accepted; partial, null, negative, overflowing or nonnumeric
phase sets are rejected. Zero fields from an older daemon mean unavailable,
not proof that either phase is free. Request and body phases are nested in
the inclusive origin timer, not additional costs. Body collection can include
stream scheduling and backend file or network reads; it is not exclusively
physical network transfer. Compare settled intervals, not partially active
snapshots, when checking phase totals against inclusive timing.

A read-only calculation from the already sealed November 16 rerun found:

| Existing Main-Interval Counter | Observed |
| --- | ---: |
| Requested metadata plaintext bytes | 2457417655 |
| Logical encrypted-origin bytes | 40573370604 |
| Origin bytes per requested plaintext byte | 16.511 |
| Origin mean milliseconds per call | 11.414 |
| Authenticated chunk evictions | 86104 |
| Sampled first fetches / refetches | 1679 / 686 |
| Sampled refetches within 1 GB of intervening fetches | 0 |
| Sampled-history evictions | 0 |

The byte ratio is a logical metadata read-amplification measure, not unique
data volume or physical NAS traffic. Sampled refetches are not an exhaustive
refetch count; their distance counters do not isolate cache capacity as the
cause. The combination warrants investigating authenticated-chunk rounding
and longer-distance reuse as well as per-fetch latency. It does not justify
a cache-policy, chunk-format or concurrency default change by itself.

Validation passed 44 encryption tests, with two existing offline measurements
ignored, and seven focused Go race tests including the authenticated native
lookup against an explicitly rebuilt debug daemon. The delayed-store fixture
independently injects request and streamed-body latency, requires the delay
in the correct phase, checks settled phase totals against the inclusive timer
and confirms cache hits leave origin phases unchanged. An initial JSON macro
recursion-limit compile failure was repaired locally by appending structured
fields after existing snapshot construction; no crate-wide limit changed.
At this implementation checkpoint no representative backup had yet run with
the split timers. The subsequent release capture is recorded below; earlier
accepted evidence and binaries remain sealed.

### Original-Source Origin Split: Failed Acceptance

A fresh pinned release ran the same 800000-path testcase directly from
`/ncl1-1-vs-50/fme_cae1`, using the qualified original-source parent and
unchanged settings and resource guards. The backup, selected restore,
encrypted reopen, unchanged-source and identity checks completed. All
799076 metadata records were reused, with zero new data blobs, changed or
failed records, publication or revision allocation. The underlying supervisor
accepted its correctness and resource checks, but the follow-up metric gate
rejected the diagnostic. There is no accepted follow-up or diagnostic receipt.

The following are provisional measurements from that failed diagnostic,
not accepted performance evidence:

| Failed Diagnostic Measurement | Observed |
| --- | ---: |
| Main backup seconds | 429.001 |
| Backup command seconds | 436.911 |
| Peak combined RSS bytes | 68589903872 |
| Inclusive origin worker-seconds | 1679.498 |
| Origin request worker-seconds | 193.788 |
| Origin body worker-seconds | 1485.363 |
| Request share of inclusive origin time, percent | 11.538 |
| Body share of inclusive origin time, percent | 88.441 |
| Origin requests / body collection attempts | 153287 / 151907 |
| Requests without body collection | 1380 |
| Body mean milliseconds per collection attempt | 9.778 |
| Logical origin bytes per requested plaintext byte | 16.510 |

The gate required equal request and body counts. A new focused native
regression, `missing_object_origin_request_has_no_body`, demonstrates that
a missing-object request records request time and an origin attempt without
ever collecting a body. That test passed, but does not establish that all
1380 unmatched requests in this capture were expected missing-object probes.
The counters in that capture cannot distinguish request errors, identity
rejection or cancellation before body collection. At the failure-review
checkpoint the gate was left unchanged. The failed run has not been
reclassified or retried; the subsequent instrumentation is recorded below.

The next bounded diagnostic is to classify request outcomes and pre-body exits
before another representative capture. The provisional body dominance points
toward body collection, backend I/O and blocking-worker scheduling; it does
not isolate physical NAS traffic or justify cache, chunk-format or concurrency
default changes. These are nested daemon worker times, not an exclusive wall
breakdown. One changed-native run is not a causal speedup comparison, and
429.001 seconds remains below the unchanged 600-second sustained gate.

The failed capture is preserved at
`/volume2/NASDA2/vaultic-test-m6/ncl1-origin-read-fme-cae1-20261116-800k`,
with a separately sealed `-failure-review` sibling. Independent review
verified build, parent and diagnostic seals, isolated capture-source and
artifact pins, reproduced the unchanged rejection and checked the remaining
integrity and resource gates. No owned executable survived. The private
runtime remains preserved as failed-attempt evidence, not reported as cleaned
up. No production data, defaults or guards changed. The capture used native
SHA-256 `eda56f90b40428f91026980ddc6f5c7499c0490be5794791eb0f09f53b9f0d04`.

### Origin Outcome Classification Follow-Up

Origin attribution now counts request successes, not-found results, other
errors and cancellations separately. Successful responses rejected by the
existing identity check have their own counter. Body collections separately
count successes, errors and cancellations. Request and body active gauges
are released even when a pending future is dropped; cancellation here includes
unwinding before a terminal result is recorded. No retry, error handling,
object identity, caching or encryption-format behavior changed.

The ten additive outcome fields travel through authenticated WriterStatus
and main-interval reporting. Terminal counters are subtracted with reset
rejection; active fields remain current gauges, not monotonic deltas. Legacy
snapshots without outcomes remain decodable. Partial, null, negative,
overflowing or nonnumeric outcome sets are rejected, as are outcome sets
without the request/body timer fields. Legacy typed zeros do not establish
that work was classified.

For settled classified intervals the supervisor requires all three identities:

- Origin attempts equal request successes plus not-found, error and cancellation outcomes.
- Request successes equal body attempts plus pre-body identity rejections.
- Body attempts equal body successes plus body error and cancellation outcomes.

Both active gauges must be zero. Other request errors, body errors,
cancellations and identity rejections still fail acceptance even if their
counts reconcile. Only explicitly counted not-found requests can explain a
successful testcase's request/body gap; the full restore, reopen, source,
zero-publication and resource gates remain required. Boundary-straddling or
inconsistent counters must not be repaired by discarding events. Legacy
captures without classifications retain the strict equality rule, so the
previous failed attempt remains rejected.

Validation passed 46 native encryption tests with two existing offline cases
ignored, eight focused Go race/integration cases and all 40 supervisor tests.
Native fixtures cover successful and missing reads, request and body errors,
dropping a future in either phase, and pre-body identity rejection. The
encrypted authenticated lookup checks nonzero, reconciled, drained outcomes
and ownership-safe reopen against a freshly rebuilt daemon. Supervisor tests
reject missing, malformed, active, inconsistent and failed outcomes while
allowing a precisely classified not-found gap. At this implementation
checkpoint no representative capture had yet run with these counters; the
subsequent attempt is recorded below. No production defaults changed.

### Original-Source Outcome Capture: Request Errors

A fresh isolated release from `e43ecbc03` plus two exact routing overlays
ran the same 800000-path testcase directly from `/ncl1-1-vs-50/fme_cae1`.
The new root, runtime, bundle and `leg-01-outcome` selector did not reuse the
previous failed attempt. Its route requires outcome classification; phase-only
telemetry is insufficient. All original-source, parent-identity, unchanged
settings, read-only mount and resource guards remained in force.

The underlying supervisor accepted the completed backup and integrity checks,
but final diagnostic acceptance failed on non-not-found request errors.
These are provisional measurements from a rejected diagnostic:

| Outcome Capture Measurement | Observed |
| --- | ---: |
| Main backup seconds | 297.331 |
| Backup command seconds | 306.370 |
| Peak combined RSS bytes | 68743921664 |
| Origin request attempts | 152850 |
| Successful requests / body attempts / successful bodies | 151904 / 151904 / 151904 |
| Not-found request results | 473 |
| Other request errors | 473 |
| Requests without body collection | 946 |
| Inclusive origin worker-seconds | 1497.382 |
| Origin request worker-seconds | 181.524 |
| Origin body worker-seconds | 1315.522 |
| Body share of inclusive origin time, percent | 87.855 |
| Body mean milliseconds per collection attempt | 8.660 |
| Logical origin bytes per requested plaintext byte | 16.509 |
| MultiGet RPC calls / failures | 112043 / 0 |

All three outcome identities reconcile. Request/body cancellation, pre-body
identity rejection, body errors and active gauges were zero. Settled outer
native snapshots also reconcile, with 489 not-found and 489 other request
errors; their wider interval is not interchangeable with the main interval.
The 473 other main-interval errors were handled below the successful native
MultiGet/RPC boundary, but that does not make them acceptable diagnostic
outcomes. The recorded counters do not identify their error subtype or source.
An expected conditional response is only a hypothesis, not an explanation
established by these measurements. The gate remains strict; no accepted
follow-up or diagnostic receipt was written, and no retry has been launched.

Restore, encrypted reopen, unchanged source, manifest and selected identities
passed for 677957 regular files and 54715539376 bytes. All 799076 metadata
records were reused, with zero changed/failed records, new data blobs,
publication or revision allocation. Independent review verified source and
isolated-build pins, build/parent/diagnostic seals, native and RPC outcomes,
remaining integrity/resource gates and the previous failed evidence. No owned
executable survived. Both failed runtimes remain preserved as evidence, not
reported as removed.

Evidence is sealed beneath
`/volume2/NASDA2/vaultic-test-m6/ncl1-origin-outcome-fme-cae1-20261116-800k`
and its separate `-failure-review` sibling. The fresh native SHA-256 is
`ce63321862489a9a396e87f8ba233ca88dd8951a58785000c64b57788095b342`.
Release qualification passed 46 encryption tests with two existing offline
cases ignored, four MultiGet and twelve timing tests, 23 focused Go race
checks, 14 compiled qualifier checks, 41 supervisor tests and seven observer
tests. Cargo was required to compile the actual isolated source tree.

The next bounded diagnostic is native request-error subtype/source attribution
and a local reproduction before another representative capture. Provisional
body dominance still points toward body collection, backend reads and
blocking-worker scheduling, but does not justify cache or concurrency changes.
These overlapping daemon worker times are not exclusive wall time or physical
NAS traffic. The failed run is not accepted performance evidence, a causal
speedup comparison, or sustained/deployment qualification; its main interval
remains below the unchanged 600-second sustained gate. Production, defaults,
the previous failed measurements and their unclassified 1380-request gap
remain unchanged.

### Bounded Origin Request-Error Details

The November 17 local follow-up adds the optional
`ciphertext_origin_request_error_counts` map without changing the existing
outcome counters or error handling. Each failed underlying request, including
not-found, records a fixed-vocabulary key:
`site/object_store_variant/condition/io_category`. Sites are `header`, `chunk`
and `range`; condition is `conditional` when any ETag or timestamp condition
is present, otherwise `plain`. Concrete object-store variants remain distinct,
including `precondition`, `not_modified`, `not_found` and `generic`.

I/O classification examines at most eight entries of the error source chain.
Recognized I/O kinds have bounded labels; other I/O kinds use `io_other`.
`io_unclassified` means no I/O error was exposed within that bound, not that
the operation performed no I/O. No object paths, backend names, credentials
or raw error messages enter the keys or payload. At most 128 distinct keys
are retained without eviction. Further events for unseen keys increment the
reserved `dropped` counter; retained keys continue counting.

Typed authenticated WriterStatus and main-interval reporting preserve the
map. Absent legacy details remain unavailable, not measured zeros. Decoding
rejects unknown keys, missing `dropped`, oversized maps, null or invalid
counters, and details without the outcome fields. Interval subtraction handles
new categories and rejects disappearing or decreasing counters without
mutating either snapshot. The supervisor additionally requires zero dropped
events and exact agreement with both the not-found and other-error totals
when details are present. A precisely classified conditional response still
fails the unchanged non-not-found request-error gate.

Local validation passed 48 encryption tests with two existing cases ignored,
10 focused Go race/integration cases against a freshly rebuilt native daemon,
and all 42 supervisor tests. Fixtures reproduce real in-memory-store ETag
precondition and not-modified responses, generic errors with an I/O cause,
bounded storage overflow, cancellation accounting and authenticated detail
forwarding. These fixtures establish discrimination, not the cause of the
473 historical errors. Neither failed capture was reclassified or rerun.
No representative backup, production modification, default or guard change
was part of this local follow-up.

### Original-Source Detail Capture: Conditional Not-Modified

After commit `f48cdc213`, a fresh isolated release plus two exact routing
overlays ran the 800000-path testcase directly from
`/ncl1-1-vs-50/fme_cae1`. The new `leg-01-detail` route required both outcome
and bounded error-detail coverage. All control settings and source, parent,
read-only namespace, resource and runtime guards remained unchanged.

Backup, restore, encrypted reopen, unchanged-source and zero-publication
checks passed. Final diagnostic acceptance still failed at the unchanged
non-not-found request-error gate. These are measurements from that rejected
diagnostic, not accepted performance evidence:

| Detail Capture Measurement | Observed |
| --- | ---: |
| Main backup seconds | 302.044 |
| Backup command seconds | 310.328 |
| Peak combined RSS bytes | 68284039168 |
| Origin request attempts | 152872 |
| Successful requests / body attempts / successful bodies | 151908 / 151908 / 151908 |
| Header not-found, plain request, I/O not-found | 482 |
| Header not-modified, conditional request, I/O unclassified | 482 |
| Dropped request-error detail events | 0 |
| Requests without body collection | 964 |
| Inclusive origin worker-seconds | 1504.023 |
| Origin request worker-seconds | 183.622 |
| Origin body worker-seconds | 1319.998 |
| Body share of inclusive origin time, percent | 87.765 |
| Body mean milliseconds per collection attempt | 8.689 |
| MultiGet RPC calls / failures | 112949 / 0 |

All 482 other main-interval request errors were precisely
`header/not_modified/conditional/io_unclassified`. The separate not-found
bucket was `header/not_found/plain/io_not_found`. Both detail totals reconcile
with the aggregate counters, with no other categories or dropped events.
Request/body cancellation, identity rejection, body errors and active gauges
were zero. The settled wider native interval recorded 497 of each response
category, not interchangeable with the main interval. All native MultiGet
and RPC outcomes succeeded.

Independent review verified actual and isolated-build pins, selection and
source identity, integrity/resource gates, detail accounting and both earlier
failed captures' raw pins and seals. No owned executable survived; the failed
runtime remains preserved. Evidence is beneath
`/volume2/NASDA2/vaultic-test-m6/ncl1-origin-detail-fme-cae1-20261117-800k`,
with a separately sealed `-failure-review` sibling. Native SHA-256 is
`5e2c6a9957c3bc45a1bed35b8af381502437cb16fbbe36858d290921317fc9eb`.
Release qualification passed 48 encryption tests with two existing cases
ignored, four MultiGet and twelve timing tests, 26 top-level Go race checks,
15 top-level compiled qualifier checks, 43 supervisor tests and seven observer
tests.

The next focused change is to prove the conditional-response handling path
and distinguish expected not-modified responses from actual request failures
in outcome accounting. No such reclassification or acceptance change was
made for this capture. Its measured variants do not retroactively identify
the previous 473 errors or the older 1380-request gap. All three diagnostics
remain rejected; no cache, concurrency, production or default change followed.
The main interval remains below the unchanged 600-second sustained gate, and
the overlapping worker timers do not establish physical NAS traffic or a
causal speedup.

### Conditional Boundary Consumer And Subcounter

The next local follow-up traced the response to SlateDB's pinned
`ObjectStoreBoundaryObject` implementation. It sends its cached ETag as
`If-None-Match` and consumes `NotModified` by reusing the cached boundary.
A missing cache is invalid state. An encrypted local-filesystem fixture now
exercises this actual consumer, proves unchanged-boundary reuse without body
collection, rejects an old ID after the boundary changes and observes deletion
through a fresh missing-object read. It uses a dev-only dependency on the
already pinned transactional-object crate; no dependency versions changed.

The optional `ciphertext_origin_request_not_modified` counter identifies
origin `NotModified` responses only when the underlying request contains
`If-None-Match` or `If-Modified-Since`. Plain requests and requests containing
only `If-Match` or `If-Unmodified-Since` do not qualify. Precondition failures
remain separate. This is a subset of the existing
`ciphertext_origin_request_errors`, not an additional terminal outcome:
aggregate counters and all existing attempt identities retain their meaning.
The aggregate minus the subcounter distinguishes the remaining request errors
when this instrumentation is available.

Typed authenticated reporting preserves legacy absence separately from a
measured zero. Interval subtraction rejects resets and does not alias source
snapshots. The supervisor validates the subcounter's type, availability and
bounds against both aggregate errors and conditional not-modified details.
It still rejects aggregate request errors, including those explained by this
subcounter. No acceptance gate or historical measurement was changed.

The header-validation cache can produce a conditional response without another
origin fetch. The consumer fixture explicitly invalidates its cached header
to reach the cold-header origin branch measured by the rejected capture;
it does not disable or change production caching. Caller-level conditional
responses are therefore not interchangeable with this origin-only counter.

Local validation passed 50 encryption tests with two existing cases ignored,
12 Go race/integration checks against a freshly rebuilt daemon and all 44
supervisor tests. All three failed captures reproduced their original strict
rejection, with their seals and raw-file pins verified unchanged. No new
representative backup, commit, default, cache or concurrency change was made.

### Explicit Conditional Origin Coverage Policy

A subsequent local follow-up adds the explicit
`allow_conditional_not_modified=True` option to the origin coverage validator.
Its default remains false. At that checkpoint no capture route opted in. Opt-in
requires the new subcounter even when zero, complete bounded details,
classified and reconciled outcomes, and drained active gauges. Only conditional
header `NotModified` details can qualify: their total must equal both the
eligible subcounter and all aggregate request errors. Thus a precondition
failure, plain not-modified response, chunk/range response, unexplained error,
partial accounting or dropped detail event cannot be exempted. Cancellation,
identity rejection and body failures remain disqualifying.

This changes only an explicitly selected origin-coverage policy, not native
aggregate counter meanings, production behavior, resource/integrity checks or
the sustained-runtime gate. Historical captures lack the new subcounter and
cannot qualify under opt-in; their default rejection remains unchanged. A
future diagnostic must record this policy choice and collect fresh telemetry
from pinned binaries rather than synthesizing the counter from old details.

### Dedicated Conditional Origin Route

The next local wiring step adds `leg-01-conditional` beneath the separate
`ncl1-origin-conditional-fme-cae1-20261117-800k` evidence root and selects only
the `phase35-ncl1-origin-conditional800k-20261117/linux-amd64` binary bundle.
Python supervision and the Go qualifier both require this exact leg, reject
alternate paths and simultaneous experiments, and retain the prior control
settings: 56 blob tasks, worker opening, 1 ms coalescing and unchanged cache,
crypto, source and resource limits. Representative input remains directly
`/ncl1-1-vs-50/fme_cae1`, never a copied source.

Only this new route selects the conditional-header policy. It requires fresh
subcounter, detail and outcome coverage in both the exact-main interval and
the wider writer-status interval. A successful follow-up receipt records the
exact selected validation flags in `origin_read_policy`; these are the same
flags passed to validation, not a policy inferred from response counts.
Historical origin routes retain their strict flags and cannot inherit the
exception from an environment override or a sibling path.

This is local route preparation, not a release build, new capture or accepted
performance result. A future run still needs a freshly pinned bundle and
create-only launch evidence. No earlier failed capture, sealed receipt,
production behavior or sustained-runtime requirement was changed.

### Original-Source Conditional Capture: Accepted Diagnostic

The subsequent fresh release and `leg-01-conditional` capture completed
directly from `/ncl1-1-vs-50/fme_cae1`, using the qualified 800000-path parent
and unchanged control settings. Both the exact-main and wider native intervals
passed the explicitly recorded conditional-header policy. Backup, restore,
encrypted reopen, unchanged-source identity, zero-publication and resource
checks passed. This is accepted diagnostic evidence under that policy, not
acceptance of an older capture or a sustained-performance qualification.

| Conditional Capture Measurement | Observed |
| --- | ---: |
| Main backup seconds | 285.185 |
| Backup command seconds | 292.644 |
| Peak combined RSS bytes | 67855626240 |
| Origin request attempts | 152831 |
| Successful requests / body attempts / successful bodies | 151909 / 151909 / 151909 |
| Header not-found, plain request, I/O not-found | 461 |
| Header conditional not-modified details / eligible subcounter | 461 / 461 |
| Aggregate request errors / unexplained request errors | 461 / 0 |
| Dropped request-error detail events | 0 |
| Requests without body collection | 922 |
| Inclusive origin worker-seconds | 1493.264 |
| Origin request worker-seconds | 175.531 |
| Origin body worker-seconds | 1317.368 |
| Body share of inclusive origin time, percent | 88.221 |
| Body mean milliseconds per collection attempt | 8.672 |
| Logical origin bytes per requested plaintext byte | 16.510 |
| MultiGet RPC calls / failures | 110035 / 0 |

The aggregate error counter still includes the 461 conditional responses;
its semantics did not change. Their eligible subcounter equals their complete
header detail count. The wider interval independently reconciled 476 not-found
and 476 conditional not-modified responses, with 152307 successful bodies.
Both intervals had zero request/body cancellations, identity rejections,
body errors, dropped details and active work at the accepted boundaries.

Independent analysis verified 35 build-source and 50 launch pins, actual and
isolated-source provenance, selected source identity, integrity/resource
checks, and all three previous failures' raw pins, seals and strict rejection.
All owned executables exited and the private runtime was removed. Evidence
is beneath
`/volume2/NASDA2/vaultic-test-m6/ncl1-origin-conditional-fme-cae1-20261117-800k`,
with a separately sealed `-analysis` sibling. Native SHA-256 is
`9a5f32e5f7d7a8ae1b66da0d447f3ab24a46c0fb4d9d699678118e3ca9416c4d`.
Release qualification passed 50 encryption tests with two existing cases
ignored, four MultiGet and twelve timing tests, 29 top-level Go race checks,
16 compiled qualifier checks, 46 supervisor tests and seven observer tests.
The isolated source and capture seals remain unchanged by this later report.

The main interval is below the unchanged 600-second sustained gate. Body
collection dominates inclusive origin worker time, but includes file/network
I/O and blocking-worker scheduling; overlapping scopes are not exclusive wall
time. Logical encrypted database reads are not physical NAS traffic or source
content reads. No causal speedup, instrument-overhead, NAS-saturation,
deployment or production-default claim follows from this single diagnostic.

### Body Payload Attribution Before Queue Timing

After the accepted diagnostic checkpoint, local instrumentation adds the
optional `ciphertext_origin_body_payloads` group with `file_calls`, `file_ns`,
`stream_calls` and `stream_ns`. Classification uses the actual returned
`GetResultPayload`, not a guessed backend identity. It leaves the dependency's
`bytes()` implementation, range behavior, scheduling and concurrency unchanged.
File timers still include blocking-worker queueing, read work and completion
scheduling; stream timers include stream waiting and materialization. The
difference between inclusive body time and these nested timers is not a queue
measurement. Neither timer is physical I/O service time.

The pinned object-store implementation sends file payloads through its private
`maybe_spawn_blocking` and `local::read_range` helpers. Exact queue/service
separation therefore needs a dependency-level instrumentation hook, not a
replacement file reader or an extra scheduling probe. At this stage no such
dependency hook had been added; the next subsection records its isolated
diagnostic qualification.

Typed authenticated reporting preserves absent legacy groups separately from
measured zero, rejects partial or malformed groups and subtracts independent
interval values with reset checks. Settled coverage requires payload attempts
to partition body attempts and nested timings to fit within the inclusive body
timer. Native tests cover actual local-file ranges, stream content, errors and
cancellation. The rebuilt authenticated lookup fixture observed three file
bodies and zero stream bodies. This is a local fixture observation, not a
retroactive classification of the representative diagnostic. Existing sealed
captures lack the new group and remain unchanged; no new representative backup
was launched for this follow-up.

### Private File-Worker Timing Hook

The opt-in native feature `diagnostic-body-timing` uses a shared, standard-library
observer crate and a private patch of the locked `object_store` 0.14.1 archive
(SHA-256 `d354792e39fa5f0009e47623cf8b15b099bf9a652fa55c6f817fe28ac84fea50`).
The preparer verifies the archive against the lockfile, archives HEAD plus pinned
workspace overlays, and emits the dependency diff, Cargo override and preparation
manifest into a new private `/run/vaultic-body-hook-*` directory. It does not
edit the Cargo registry or install a workspace-wide override. Production/default
dependency selection, worker counts, crypto pools and caching policy stay unchanged.

The patch only observes the existing file branch of `GetResult::bytes()`. It
keeps the existing `maybe_spawn_blocking` and `local::read_range` implementation,
including positional reads, retries, range limits and errors. The stream branch
is unchanged. The optional `ciphertext_origin_file_read` group contains:

- `queue_ns`: observer submission to the actual worker's entry. This includes
   observer/task setup and dispatch overhead, not solely time in Tokio's queue.
- `service_ns`: the unchanged `read_range` invocation inside that worker,
   including buffer allocation and kernel/filesystem waits, not pure disk service.
- `completion_ns`: read completion to awaiter resumption, including observer
   accounting, file-handle cleanup, worker completion and scheduling overhead.
- `calls`, `started`, `finished`, `resumed`, `active`, `errors`, `panics`,
   `abandoned`, `cancellations` and `direct_calls`: independent lifecycle coverage.

Cancelling the awaiter does not necessarily cancel queued or running blocking
work. Its worker remains active until the actual read finishes or is abandoned;
its later service time is still recorded. Live snapshots are independently loaded
atomics, not transactional or necessarily reconciled. Accepted settled intervals
require a drained baseline, matching calls/starts/finishes/resumptions and file
payload counts, no errors, panics, abandonment, cancellation or direct fallback,
and queue plus service plus completion time bounded by inclusive file-body time.
These are overlapping worker times, not additive wall time or physical NAS traffic.

The native group is absent until an actual hooked file read occurs. Default
builds and feature-enabled builds without the private patch leave it absent,
not measured zero. Go forwarding and deltas preserve that distinction; the
supervisor rejects availability changes and malformed or unsettled groups.
An explicit `require_file_read=True` additionally rejects absent or unmeasured
coverage. Historical route policy is unchanged. The observed-baseline policy
below qualifies availability without inferring a zero baseline or silently
warming caches merely to make the group appear.

From the workspace root, prepare a fresh isolated tree and run the actual-worker
tests explicitly (they are ignored in ordinary feature-only runs):

```sh
root="/run/vaultic-body-hook-$(date +%Y%m%dT%H%M%S)-$$"
env PYTHONDONTWRITEBYTECODE=1 python3.12 helpers/phase35-m6/body_timing.py --output "$root"
env TMPDIR=/run CARGO_TARGET_DIR=/run/vaultic-cargo-target \
   cargo --config "$root/cargo-config.toml" test --offline \
   --manifest-path "$root/candidate/vaulticdb/Cargo.toml" \
   --features diagnostic-body-timing --lib encryption::tests::origin_file_read_diagnostic \
   -- --ignored --test-threads=1
```

The first private build changes only the candidate lockfile's object-store entry:
it selects the emitted local source and adds the observer dependency edge.
Inspect those changes, then use `--locked` on subsequent commands. Never rerun
the create-only preparer over an existing directory.
Retain the preparation manifest and source pins with any diagnostic artifact.

Local qualification occupied the runtime's sole blocking worker and observed
`calls=1, started=0, active=1` before releasing it. Separate tests verified that
cancelling the queued awaiter retains the worker, and that read errors match the
unchanged dependency behavior. Observer lifecycle tests cover abandonment, panic
and no-runtime fallback. The full patched encryption suite and supervisor suite
passed. The real authenticated native lookup also reconciled three hooked file
reads and zero stream reads; `VAULTIC_BODY_TIMING_EXPECT_HOOK=1` makes that fixture
require hook coverage. This is diagnostic plumbing qualification, not evidence
about the earlier representative capture's queue/service split. No new
representative backup, speedup, saturation or deployment claim follows. Any
future representative backup must still use `/ncl1-1-vs-50/fme_cae1` directly.

### Observed File-Read Baseline Policy

`qualify_file_read_baseline` accepts a raw authenticated writer-status envelope
only when it contains a positive native process start timestamp, measured hook
coverage and fully reconciled, settled origin/file-read counters. It returns an
`observed-settled-file-read-v1` receipt with that timestamp, the actual baseline
file-call count and the explicit conditional-response policy. Missing hook
coverage is rejected even when every payload counter is zero: absence alone
does not prove that the private dependency patch is installed. No synthetic
zero counters, warm-up read, reader replacement or cache policy change is used.

`qualify_file_read_interval` checks both writer-status envelopes under the same
policy, requires the native start timestamps to match, computes reset-checked
deltas and qualifies the resulting interval. The receipt includes baseline and
endpoint file-call counts and the interval's encryption metrics. A valid
baseline is not itself a measured interval: zero additional file reads are
rejected rather than reported as zero queue/service cost. Conditional
`NotModified` remains rejected unless the caller explicitly opts into its
existing fully reconciled policy; baseline qualification does not relax it.

The authenticated local lookup fixture now records status immediately after
normal repository opening and before lookup. That opening naturally supplied
three completed hooked file reads, and the Python qualifier accepted the actual
status, not a hand-written envelope. The empty-catalog lookup added no file
reads, and the same qualifier correctly rejected its interval as unmeasured.
Separate policy tests cover positive intervals, missing or zero-work baselines,
unsettled work, timing bounds, counter resets and native process replacement.
The full 49-test supervisor suite, Go race/authenticated fixture and four
current observer lifecycle tests passed. The intervening observer edit was
preserved and verified as formatting-equivalent to the private daemon's source.

This establishes a local baseline policy, not a new representative result.
The qualifier is explicit and is not retrofitted onto historical routes or
sealed captures. A future dedicated hook capture must invoke the baseline gate
before measurement and retain its receipt with the raw statuses, then qualify
the measured endpoints and main interval. If normal opening does not supply a
settled observed baseline, stop before the backup instead of adding hidden
warm-up work or inferring missing counters. Any representative backup still
uses `/ncl1-1-vs-50/fme_cae1` directly. No new representative backup was launched.

### Dedicated File-Timing Capture Route

The new route is `leg-01-file` beneath
`/volume2/NASDA2/vaultic-test-m6/ncl1-origin-file-fme-cae1-20261117-800k`,
with runtime `/run/vaultic-unchanged-ncl1-origin-file800k-leg-01-file` and bundle
`bin/phase35-ncl1-origin-file800k-20261117/linux-amd64`. Both Python and the Go
qualifier require that exact route, the 800k profile and its own bundle; child
paths, mixed experiments and wrong profiles are rejected. Control settings,
including 56 blob tasks, worker opening, caching, crypto and 1ms coalescing,
are unchanged. Representative input remains `/ncl1-1-vs-50/fme_cae1` directly.

After the existing identity preflight and idle-writer check, but before invoking
the backup command, the Go runner calls `qualify_file_read_capture` through
Python 3.12. This reuses the observed-baseline policy rather than implementing a
second copy in Go. Missing, unmeasured or unsettled hook coverage stops the run
before backup. No extra repository/cache-warming read is added. Each Python
gate has a 30-second bound; the existing main runtime, sustained-duration and
resource guards remain unchanged.

Before backup, the runner saves `file-read-baseline.json` with raw writer status
and its qualification receipt. After backup and before restore verification,
it qualifies both the wider writer-status interval and the actual CLI main
interval, then saves `file-read-interval.json` with both raw statuses, the main
telemetry and the qualification receipt. Files are created exclusively, chmodded
0600 and synced; an existing receipt cannot be overwritten. Missing main hook
metrics or zero file-work intervals cannot pass merely because startup reads
proved the baseline capability.

Independent analysis recomputes both qualifications from the raw measurement
and requires exact agreement with both saved receipts. It rejects missing,
edited, symlinked, non-private or over-2MiB receipt files. The new route's report
includes `file_read_qualification`; historical report and policy shapes remain
unchanged. The existing follow-up seal already covers both receipt files.
Only this route requires file-read coverage in addition to the prior explicit,
fully reconciled conditional-response policy; historical routes and sealed
evidence are not reinterpreted.

Local validation passed all 50 supervisor tests and six Go race/authenticated
cases. The real pre-backup bridge accepted the naturally observed three-read
baseline and rejected the empty lookup's zero-file-work interval. Tests also
verified private create-only receipt persistence, exact-route control settings,
main-interval requirements and independent evidence tamper rejection.

At the wiring checkpoint this route was locally qualified, not built or
captured. The isolated release build requires the private pinned dependency patch plus
`diagnostic-body-timing`, and pin the Go qualifier and runtime Python helper as
well as the native daemon and CLI. The bridge imports the helper from
`/root/proj/vaultic/helpers/phase35-m6`; retain and verify its source pin for the
entire run. Older release bundles and already-executed create-only drivers
must not be reused for this route. No new representative backup or performance
claim is made by this wiring change.

### File-Timing Release Qualification

The November 17 isolated release build completed successfully from
`/run/vaultic-body-hook-20261117-release/candidate`. Its new sealed bundle is
`bin/phase35-ncl1-origin-file800k-20261117/linux-amd64`. The create-only build
driver and preparer have each run once; do not rerun them over these artifacts.
The daemon was explicitly recompiled from this source with
`diagnostic-body-timing` and the private checksum-pinned `object_store` patch.
The default workspace lockfile and registry source remain unchanged by the
build. Only the private candidate selects the patched source and its observer
dependency edge; no dependency versions changed.

Release qualification passed 52 encryption tests, three actual worker
queue/cancellation/read-error tests, four observer lifecycle tests, four native
MultiGet tests and 12 native attribution tests. It also passed 32 Go race cases,
16 cases from the copied qualifier executable, all 50 supervisor tests and seven
host-observer tests. Both Go qualification passes used this bundle's release
daemon and obtained a real observed-baseline receipt from the pre-backup bridge.
No backup workload was launched by these build gates.

SHA-256 pins:

- CLI: `c7b9ef43a9335578d27af9408575d89ae77ce169eec8e8c60d723b1350b340e5`.
- Native daemon: `e7ec48bef50a26695636da8f10bcf1473c1f34178b1a685be3a00d53e1e68c8d`.
- Qualifier: `6549b1cbc5ed8dc90f8def93ad8115f5941cacd494e9c384b8d9e74d04bb64dc`.
- Build manifest: `e2499c61dfda0ba490d629b7548ea65da575c5503219cf496cff2648d323779e`.
- Runtime Python helper: `67eddf019919440f8b1f3f121a8b0a54a2aaa17f7d3ff454024294869cdaaebe`.

Independent verification checked the bundle seal, all 38 source pins, exact
diagnostic feature/policy, required executed case counts and private artifact
modes. The new representative capture root remained absent. This report was
appended after sealing: the workspace operations document is therefore newer
than its captured source pin. Use the retained isolated source to verify the
build's documentation bytes; do not rewrite the build seal or manifest to match
this later report. Runtime helper and executable pins must still match before
any capture.

The bundle is qualified diagnostic tooling, not a representative queue/service,
speedup, NAS saturation or deployment result. A fresh guarded capture and analysis
driver is still required, using `/ncl1-1-vs-50/fme_cae1` directly, the exact new
route, observed-baseline receipts and unchanged resource/runtime guards. Older
capture drivers and evidence remain untouched. No representative backup was
launched for this release qualification.

### File-Timing Capture Postmortem

After commit `b8f2f3ddf`, fresh guarded drivers launched the exact `leg-01-file`
route directly against `/ncl1-1-vs-50/fme_cae1`. The backup and integrity runner
completed, but the enclosing diagnostic failed during post-backup live-window
analysis: `origin file-read interval begins with active work`. The original
failure record and seal remain unchanged; this capture is not accepted.

The failure applied a settled-baseline requirement to a periodic live sample.
`encryption_counter_deltas` now retains its strict default and accepts an explicit
`require_settled_file_read=False` only at the live bottleneck-window call site.
Unsigned shape, availability and monotonic counter checks remain enforced; active
is still an end gauge. Live windows do not qualify settled worker timing. The
existing regression test now covers an active live baseline, strict-default
rejection, gauge preservation and reset rejection. All 50 supervisor tests pass.

Separate immutable postmortem evidence is retained at
`/volume2/NASDA2/vaultic-test-m6/ncl1-origin-file-fme-cae1-20261117-800k-failure-review`.
It reproduces the failure with the captured helper, verifies both original strict
file-read receipts, replays all nine live windows using the fixed helper and pins
the raw evidence without writing acceptance files into the failed capture.

The retained main interval contains 151914 reconciled file reads, with no worker
errors, panics, abandonment, cancellation, direct fallback or active work:

| Measured Scope | Worker Seconds | Mean Milliseconds | Inclusive File-Body Share |
| --- | ---: | ---: | ---: |
| Submission to worker entry | 3.053 | 0.02010 | 0.224% |
| Unchanged range reader | 1354.612 | 8.91697 | 99.351% |
| Read completion to awaiter resumption | 5.104 | 0.03360 | 0.374% |

This rejected capture's retained timing is service-dominated, not queue-dominated.
Service includes allocation and kernel/filesystem waits, not pure disk service;
submission includes observation/dispatch overhead, and completion includes cleanup
and scheduling. These overlapping worker scopes are not exclusive wall time or
proof of physical NAS traffic, saturation, speedup or instrumentation overhead.

The main interval was 289.180729973 seconds, the command 297.292960270 seconds,
and peak combined RSS 68925988864 bytes. All 800000 selected entries, 677957
regular files and 54715539376 bytes matched the source receipts. Restore,
encrypted reopen, unchanged-source and zero-new-data-blob checks passed. No owned
processes survived; only an empty private owned runtime directory required
removal after failed finalization. The main remains below the unchanged
600-second sustained gate. This postmortem does not reclassify the capture or
replace a fresh fully accepted diagnostic. The fixed helper and this later report
are intentionally newer than the sealed build and launch pins; preserve those
original artifacts and use fresh qualified provenance for a future capture.

### Fresh File-Timing Route With Live-Window Analysis

The follow-up to the rejected file-timing capture uses a new exact namespace:

- Root: `/volume2/NASDA2/vaultic-test-m6/ncl1-origin-file-live-fme-cae1-20261117-800k`.
- Label: `leg-01-file-live`.
- Captured runtime (rejected below): `/run/vaultic-ncl1-file-live800k-leg-01-file-live`.
- Bundle: `bin/phase35-ncl1-origin-file-live800k-20261117/linux-amd64`.

The original file-timing route remains selectable under its original exact label
and strict policy; its rejected capture and postmortem seals are not rewritten.
Both file routes require classified outcomes/details, explicit eligible
conditional-response accounting, observed settled file-read baselines and
qualified main plus wider intervals. Only periodic live-window subtraction opts
out of the active-baseline restriction. Cache, crypto, concurrency, source
identity, resource ceilings and runtime guards remain unchanged. Mixed labels,
roots, alternate profiles and simultaneous cache experiments are rejected.

At wiring qualification, Python checks and both Go policy subtests passed but
did not compare the Python runtime path with the Go ownership-prefix contract.
The subsequent guarded attempt and correction are recorded below. Any
representative run must read `/ncl1-1-vs-50/fme_cae1` directly. This route-wiring
entry does not claim that a fresh capture completed or passed the sustained gate.

### File-Live Runtime Guard Postmortem

The fresh file-live attempt was rejected before the measured backup started:
`retry requires a private canonical runtime`. The captured Python runtime path
omitted `unchanged`, while the existing Go qualifier requires a canonical private
runtime under `/run/vaultic-unchanged-*`. The ownership guard worked as intended;
it was not relaxed. No observed file-read baseline, main interval or performance
result was produced by this attempt. Qualifier exit status was 1 after
540.1126794330776 seconds of setup, with peak combined RSS 715882496 bytes.

The Python route now returns
`/run/vaultic-unchanged-ncl1-file-live800k-leg-01-file-live`. Both existing Go
file-policy subtests now invoke the actual Python route selector and reject a
runtime path outside the qualifier's ownership prefix. These Go race subtests
and all 50 supervisor tests pass. The original route-only fixtures were
self-consistent but missed this cross-language contract.

Separate immutable rejection evidence is retained at
`/volume2/NASDA2/vaultic-test-m6/ncl1-origin-file-live-fme-cae1-20261117-800k-failure-review`.
It binds the captured helper and qualifier source to their launch pins,
reproduces the prefix mismatch, verifies that the guard is unchanged and records
the absent baseline/main files. The original failed capture and seal remain
unchanged. No owned executable survivors were found; only its empty private owned
runtime directory was removed. The corrected runtime path remains absent.

The runtime correction and regression test postdate the sealed file-live bundle
and launch source. Do not replace their artifacts, reuse the failed namespace or
reclassify this attempt. A future backup requires a new qualified bundle and
fresh namespace carrying this correction; the review itself launches no backup.

### Runtime-Preflight File-Timing Route

The next isolated attempt has its own exact namespace:

- Root: `/volume2/NASDA2/vaultic-test-m6/ncl1-origin-file-runtime-fme-cae1-20261117-800k`.
- Label: `leg-01-file-runtime`.
- Runtime: `/run/vaultic-unchanged-ncl1-file-rt800k-leg-01-file-runtime`.
- Bundle: `bin/phase35-ncl1-origin-file-runtime800k-20261117/linux-amd64`.

The prior file and file-live routes remain distinct, with their original failed
evidence preserved. All three file routes retain the same strict observed
baseline, main/wider receipt, outcome, conditional-response and unchanged-source
requirements. Cache, crypto, concurrency, resource and timing controls are
unchanged. Python route coverage includes exact-label isolation, tuning parity
and the native socket-path length bound; all three Go policy subtests check the
actual Python runtime output against the qualifier ownership prefix.

Before invoking the qualifier and its parent clone, the fresh launcher must
validate the prepared runtime's canonical path, unchanged ownership prefix,
private 0700 directory mode, current owner and socket-path bound, and retain a
private preflight receipt. The original qualifier guard remains authoritative
and unchanged. Fresh source, executable, helper, parent and driver pins are
required; this route entry alone makes no capture or performance claim. The
representative source remains `/ncl1-1-vs-50/fme_cae1` directly.

### Accepted Runtime-Preflight File-Timing Capture

The fresh `leg-01-file-runtime` capture completed and independent analysis
accepted it at
`/volume2/NASDA2/vaultic-test-m6/ncl1-origin-file-runtime-fme-cae1-20261117-800k-analysis`.
It read `/ncl1-1-vs-50/fme_cae1` directly, selecting 800000 entries, 677957 regular
files and 54715539376 bytes. Original source identity `1048655:3014197312` and
parent snapshot
`bf1a0adba4175cda729572a6873ddae4695d944e21795f11587a2d44fc13a0a9`
matched their qualified receipts. No copied source or translated identity was
used.

The main interval was 289.324338768 seconds, the command 297.457788171 seconds,
and peak combined RSS 68493049856 bytes. Restore, encrypted reopen and unchanged
source checks passed. There were zero new data blobs/bytes, revision-allocation
calls, reserved revisions or assigned inode revisions. Production was not
modified. The early runtime receipt records private 0700 ownership and an
81-byte socket-path bound before qualifier setup. All owned runtimes and
executables were absent after completion.

An observed, settled baseline contained 190 file reads; the endpoint contained
152494, yielding 152304 wider-interval reads. The independently qualified main
interval contained 151906 file reads and no stream payloads. In both intervals,
calls, starts, finishes and resumptions reconciled exactly; errors, panics,
abandonment, cancellation, active work and direct fallback were zero.

| Main File Scope | Worker Seconds | Mean Milliseconds | Inclusive File-Body Share |
| --- | ---: | ---: | ---: |
| Submission to worker entry | 3.265390182 | 0.02150 | 0.2271% |
| Unchanged range reader | 1428.296041621 | 9.40250 | 99.3348% |
| Read completion to awaiter resumption | 5.506126572 | 0.03625 | 0.3829% |

The measured file-body scope is service-dominated, not dispatch/queue-dominated.
Submission includes observation and dispatch setup; service includes unchanged
`read_range` allocation and kernel/filesystem waits; completion includes
accounting, handle cleanup and scheduling. These are nested worker durations,
not an additive wall-time breakdown, pure disk service or proof of NAS
saturation. This is not a paired speedup or instrumentation-overhead estimate.

Main requests reconcile as 152836 attempts: 151906 successes, 465 NotFound and
465 fully classified eligible conditional NotModified responses. The latter
retain their aggregate request-error semantics. Body successes equal 151906,
with no unexplained errors, cancellation, identity rejection or dropped details.
The wider interval independently reconciles 152304 successes and 481 of each
classified pre-body response. Logical origin bytes per requested plaintext byte
were 16.51127324830901, not a physical NAS traffic measurement.

Independent analysis verified build, parent, diagnostic, follow-up and analysis
seals, all 38 build-source pins, 53 launch pins, both raw file-read qualification
receipts, early runtime preflight and all correctness/resource gates. All five
earlier failed captures retain their raw evidence and original rejection. The
earlier conditional capture remains accepted under its own policy without
inferred file-worker coverage.

The isolated release from `b8f2f3ddf` plus pinned overlays passed 52 encryption,
3 actual worker-hook, 4 observer lifecycle, 4 native MultiGet, 12 native
attribution, 32 top-level Go race, 16 compiled qualifier, 50 supervisor and 7
host-observer cases. Its build manifest SHA-256 is
`714c2c837c6b5b7f62380a2b105759ba38ca5697224ddb895be67039af074a80`.
Diagnostic and follow-up manifest hashes are
`4dcc5e663e0d943ccb94b060d1acc58449dea5b68ea8e55a6924ffdfa2538403`
and `75baf6c6b431623ab88a85dee5b5091cd4cdd215d453cd334c96d762c0dcaeb6`.
This report postdates sealing; use the retained isolated source for the captured
documentation pin rather than rewriting the original manifests.

The 289-second main interval remains below the unchanged 600-second sustained
gate. Accepted diagnostic evidence is not sustained-performance or deployment
qualification; no defaults, resource ceilings, cache policy, concurrency or
production configuration changed.

### Range-Reader Stage Diagnostic

The next isolated diagnostic uses exact route `leg-01-file-range` beneath
`/volume2/NASDA2/vaultic-test-m6/ncl1-origin-file-range-fme-cae1-20261117-800k`,
bundle `bin/phase35-ncl1-origin-file-range800k-20261117/linux-amd64`, and private
runtime `/run/vaultic-unchanged-ncl1-file-rg800k-leg-01-file-range`.
It retains direct `/ncl1-1-vs-50/fme_cae1` input and all existing cache,
concurrency, encryption, resource, correctness and sustained-duration gates.
The prior accepted capture and five rejected attempts remain immutable.

The create-only dependency preparer now also patches the locked private
`object_store` reader in place. The normal registry implementation and default
dependency selection remain untouched. Existing readers call the same body
with no observer; only the actual observed File-body worker supplies one.
Allocation observes unchanged `Vec::with_capacity`, zero-fill observes unchanged
`resize`, each positional-read phase surrounds the existing `read_at` or
`seek_read`, and conversion observes unchanged `Vec` to `Bytes` conversion.
Range arithmetic, buffers, read loop, retry/error/EOF handling and returned bytes
are preserved; there is no replacement reader, extra blocking task or probe I/O.

The optional `ciphertext_origin_file_range_read` group is emitted only after
actual observed range-reader entry. Its 21 counters distinguish lifecycle,
requested/returned bytes, empty/unsupported paths, read errors/interrupts/EOF,
and calls/durations for the four stages. Absence is unavailable, not zero.
Standalone lifecycle tests and real hooked-reader success, cancellation, error,
empty and EOF fixtures check exact bytes, error identity and nested durations.
Transport parsing rejects malformed groups, while live counter subtraction
remains separate from settled qualification.

Only this fresh route requires `observed-settled-file-range-read-v1`: a positive
observed baseline, unchanged native process identity, drained lifecycle and
matching worker/range/payload calls at baseline, main and wider boundaries.
Requested and returned range bytes must match; failed, panicked, empty,
unsupported, interrupted or EOF work cannot qualify. Stage counts must
reconcile, stage durations must fit within range duration, and range duration
must fit within existing worker service. Exact raw receipts remain mandatory.

The positional-read scope includes syscall overhead, copying into the user
buffer, faults, scheduler/kernel/filesystem waits and any remote I/O. It cannot
separate NAS service from kernel copying, prove physical traffic or infer NAS
saturation. The residual range duration contains control flow, bookkeeping and
instrumentation, not a pure overhead measurement. All durations are nested
worker time, not an additive wall-time decomposition or a paired speedup claim.
No representative stage breakdown is claimed before capture and independent
analysis complete.

### Accepted Range-Reader Stage Capture

The fresh `leg-01-file-range` capture and independent analysis completed
successfully. Sealed analysis resides at
`/volume2/NASDA2/vaultic-test-m6/ncl1-origin-file-range-fme-cae1-20261117-800k-analysis`.
Input remained direct `/ncl1-1-vs-50/fme_cae1`: 800000 selected entries, 677957
regular files, 54715539376 bytes and original identity `1048655:3014197312`.
The qualified original parent snapshot remained
`bf1a0adba4175cda729572a6873ddae4695d944e21795f11587a2d44fc13a0a9`.
The main interval was 517.422774588 seconds, command 524.655678705 seconds,
and peak combined RSS 68992991232 bytes. Restore, encrypted reopen and unchanged
source checks passed, with zero new data blobs/bytes, revision-allocation calls,
reserved revisions and assigned inode revisions. Production was not modified;
all owned runtimes and executables were absent after completion.

An observed settled baseline contained 191 file/range reads and the endpoint
152500, yielding 152309 wider-interval reads. Main calls, starts, finishes and
resumptions reconciled at 151911, with no stream payloads. Each main range read
had one allocation, zero-fill, positional syscall and conversion; requested and
returned range bytes both equalled 40574419244. All active, failed, panicked,
abandoned, cancelled, empty, unsupported, interrupted and EOF counters were zero.
Wider range calls and bytes independently reconciled at 152309 and 40755027460.

| Main Range Scope | Worker Seconds | Mean Milliseconds | Range-Time Share |
| --- | ---: | ---: | ---: |
| Vector allocation | 2.084753698 | 0.01372 | 0.1480% |
| Zero fill | 6.851454305 | 0.04510 | 0.4863% |
| Positional `read_at` | 1399.502101963 | 9.21264 | 99.3417% |
| Vector to `Bytes` conversion | 0.053904008 | 0.00035 | 0.0038% |

Total range duration was 1408.776605048 worker seconds, with 0.284391074 seconds
(0.0202%) outside the four measured stages. Existing worker queue, service and
completion were 3.203844448, 1408.909937271 and 5.335391759 seconds respectively.
The observed range scope is positional-read dominated; allocation, zero-fill
and conversion together account for about 0.638% of that scope. These nested
worker durations are not an additive wall-time breakdown. In particular,
`read_at` still includes kernel copying, faults, scheduling and filesystem or
remote-I/O waits, not pure NAS service; neither physical traffic nor NAS
saturation is established. The residual is not calibrated instrumentation
overhead. Do not infer instrumentation overhead or a causal speedup/slowdown
from this unpaired run or its different main duration.

Main requests independently reconciled at 153563: 151911 successes, 826
NotFound and 826 fully classified eligible conditional NotModified responses.
The latter retain aggregate request-error semantics. Wider requests reconciled
at 153989: 152309 successes and 840 of each classified pre-body response.
There were no unexplained failures, body errors, identity rejections,
cancellations or dropped details. Logical origin bytes per requested plaintext
byte were 16.510347891360187, not a physical NAS traffic measurement.

Independent analysis verified bundle, parent, diagnostic, follow-up and analysis
seals, all 38 build-source and 53 launch pins, exact raw baseline/main/wider
receipts, early private-runtime preflight and all correctness/resource gates.
All five rejected captures remain rejected; both earlier accepted conditional
and file-runtime captures retain their own policies without inferred worker or
range-stage counters. The isolated release from `efbea0b32` plus pinned overlays
passed 52 encryption, 5 real hook, 7 observer lifecycle, 4 native MultiGet,
12 native attribution, 34 top-level Go race, 16 compiled qualifier,
51 supervisor and 7 host-observer cases. Default and feature-only/unpatched
File/Stream compatibility also passed separately without fabricated coverage.

Build manifest SHA-256 is
`febc19ada059d59a5e1550de9528c76534eb30f6daab90ca600d1c35ac80d1a4`;
diagnostic and follow-up manifest hashes are
`8d56fdecc4cc9df617d9d7e7e7ec4833df60eef94d1b8dde2aecad72b374f613`
and `2d81484252cdf25c325b2170db9ccc36b39d1aad4247c57cefd77ed44d5cbecb`.
This report postdates sealing; retained isolated documentation remains the
original build/capture pin. No existing manifests or receipts were rewritten.

The 517-second main interval remains below the unchanged 600-second sustained
gate; this is accepted diagnostic evidence, not sustained-performance or
deployment qualification. No production defaults or resource policy changed.
These measurements do not support allocation or buffer conversion as the first
optimization target. The next diagnostic should attribute positional-read
latency across kernel/filesystem/scheduling scopes and separately explain main
wall time before selecting an optimization or claiming a paired improvement.

### Kernel Trace Preflight

The two accepted, unpaired captures give different leads for main wall time
and metadata reader time. Main time rose from 289.324 to 517.423 seconds, while
accumulated metadata reader service fell from 1428.296 to 1408.910 worker
seconds. Writer wait for prepared work rose from 124.836 to 373.009 seconds.
At equal operation counts, source content-open duration rose from 342.020 to
739.402 worker seconds and directory duration from 128.006 to 367.690 worker
seconds. These scopes overlap and do not establish the cause of the wall-time
change. Keep direct NCL1 source traversal separate from metadata fixture reads
on the NASDA2 NFS mount when attributing latency.

A read-only host preflight recorded syscall entry/exit and `nfs_file_read`
events for four controlled 1024-byte positional reads on an existing NASDA2
evidence file. All four NFS events matched the opened device and inode, expected
offsets and byte counts between the corresponding syscall boundaries on the
same thread. This identifies NFS client reads, not cache misses or RPCs. The
fixture is not a representative backup or an NCL1 latency measurement.

Task-scoped `sched_switch` alone exposed switch-outs but not paired switch-ins.
Task-scoped `perf record --switch-events` captured both edges around an owned
process's controlled 20 ms blocking interval. A concurrent pipe fixture
matched one switch pair inside each of two overlapping read syscalls, including
one thread that resumed on a different CPU. Join by thread, not CPU.

A separate read-only NASDA2 probe used `O_DIRECT` on the same evidence file
to exercise actual off-CPU time inside positional reads without dropping host
caches. Two threads each returned 4096 bytes at distinct offsets; both reads
overlapped and each had one matching NFS client read event and one switch pair
inside its syscall. Their off-CPU intervals were 23.278 and 48.150 ms. This
qualifies the join in a small fixture, not the production buffered read path
or a stable latency estimate. Off-CPU time includes both blocked time and
runnable delay; these records alone cannot split them or identify an NFS RPC.

A separate PID-attachment fixture began recording disabled and waited for
perf's enable acknowledgment before allowing reads. It retained one exact
16-byte positional read from an existing worker and one from a worker created
after attachment. The fixture waited for perf's disable acknowledgment before
stopping the tracer. Verify that same coverage against the owned daemon and
the backup interval before interpreting production-path results.

Bound trace size and runtime, run the decoder noninteractively, reject lost or
unmatched records, and leave asynchronous NFS/RPC work unattributed until
independently correlated. Any representative backup must still use
`/ncl1-1-vs-50/fme_cae1` directly under the existing guards. No cache dropping
or host tracing configuration changes were needed for these preflights.

The separate `leg-01-file-perf` qualifier route attaches only to the daemon
owned by the test process and verifies Linux process start ticks before and
after recording. Enable and disable acknowledgments bracket the backup
command. Trace output stays in the private local runtime during measurement,
with a 20-minute deadline and a 256 MiB cap; it is copied to the fresh
evidence leg only after the recorder stops. A bounded decode rejects reported
loss or unequal positional syscall totals. The supervisor also requires the
private staged artifacts and a syscall count at least as large as the
authenticated main range-reader count. This aggregate floor is not per-read
coverage: the tracer also sees other positional reads in the command window.

The compiled recorder smoke test observed a known positional read. The
authenticated native smoke test verified attach and detach on the same daemon
without replacing it; that warm fixture generated no new range-reader reads.
The independent decoder then reproduced the retained direct-I/O probe's two
reads and 8192 bytes and reconciled a new buffered probe's two reads and 2048
bytes. It rejected a trace recorded without task switch events. For the traced
route it joins by process and thread, checks offset and byte count against the
NFS event, and accepts only regular object files under the disposable metadata
and WAL stores. Its read and byte totals must exactly match a separate settled
counter receipt taken at the backup command boundary, before snapshot
inspection. This is a command-window comparison, not a reconstruction of the
internal main interval. A missing object, incomplete trace, or count mismatch
leaves the diagnostic unqualified.

The first representative traced attempt ran directly from
`/ncl1-1-vs-50/fme_cae1`. The backup command finished in 303.926350017 seconds
and reported 677957 files and no new data. The qualifier rejected the run at
`perf disable: write |1: broken pipe` before restore or encrypted reopen, so
the backup command result is not an accepted integrity or off-CPU result.
Independent review verified all 25 launch pins, the failure seal, and no
surviving owned processes. The previous cleanup discarded the local recorder
files, so the recorder exit cause is unknown. A separate small-limit perf
probe demonstrated early exit and buffered overshoot, but does not establish
that the representative failure hit that cap. The recorder now preserves a
bounded rejected trace, its log and process status separately from accepted
artifacts; both normal shutdown and early-exit smoke tests passed.

Separate calibration used 56 synthetic reader threads and 152000 positional
reads on a retained NASDA2 evidence file, not a backup source. At 128 MiB,
one scheduler yield per read produced an accepted 88190022-byte trace; four
yields exhausted the cap and closed the control pipe. This reproduces the
symptom but does not establish why the representative attempt failed. At
256 MiB, four yields produced a 163503662-byte trace that reconciled all
152000 reads and 155648000 returned bytes. Eight yields produced a
265816094-byte trace within the cap, but decoding found 152002 syscall exits
for 152000 entries. The two extra exits repeated the preceding matched exit's
thread, timestamp and return value. That capture remains rejected: a size
check and recorder acknowledgments do not establish completeness.

For this perf version, PID attachment records the switch capability on
`dummy:u` and can report CPU `-01` for switch records. Launch mode records
the capability on the syscall event. The decoder accepts both layouts and
joins switch edges by thread, not CPU. Neither calibration measures NCL1
backup latency or physical NFS traffic.

The 256 MiB NCL1 retry (`leg-02-file-perf`) completed the backup command in
300.119859405 seconds, but the qualifier rejected recorder shutdown before
restore or encrypted reopen. The retained log shows disable was acknowledged;
the recorder was killed below the cap with 243423456 bytes on disk and no
capture-deadline expiry. Perf rejected the file header because its data size
was still zero. The qualifier separately allowed only five seconds for perf
to finalize after disable. Independent review verified all 27 launch pins and
no surviving owned processes. The partial raw recording is not analyzable
evidence of syscall or scheduler timing.

Finalization now has a separate 60-second bound and a distinct timeout error.
Build-ID collection is disabled because the tracepoint decoder does not use
it. A fresh seven-yield calibration finalized a 242198974-byte recording in
1.27 seconds after SIGINT and matched all 152000 reads and 155648000 returned
bytes. This qualifies the recorder change at similar output volume, not the
representative backup or the exact cause of the earlier shutdown delay.

The next representative attempt (`leg-03-file-perf`, series
`ncl1-origin-file-perf-fme-cae1-20261118-800k-retry3`) passed from
`/ncl1-1-vs-50/fme_cae1` directly. It covered 800000 selected entries and
677957 regular files, with no new data blobs or revision allocation calls.
Restore, encrypted reopen, unchanged source, and runtime cleanup passed.
Independent replay verified all 28 launch pins and the sealed raw trace.
Main time was 294.240 seconds, command time 301.505 seconds, and peak
combined RSS 68030803968 bytes. It did not meet the 600-second sustained gate.

The 246084278-byte trace had 152358 balanced positional syscall pairs,
including 152317 reads on approved metadata and WAL objects. Those reads and
their 40757124740 returned bytes matched the authenticated command-window
counters exactly. They spent 1275.285 aggregate thread-seconds inside the
syscall, including 1237.946 seconds off-CPU (97.07%, or 8.127 ms out of
8.373 ms per matched read on average). That locates most of the measured
positional-read time off-CPU. It does not distinguish blocked waits from
runnable delay, identify NFS RPCs or cache misses, or measure time exclusive
to the backup critical path. This was a single traced run, not a paired
speedup comparison.

An opt-in `--split-preempt` replay reproduced all of those sealed totals.
Only 80457718 ns of the off-CPU time (0.0065%) followed switch-out records
explicitly marked `preempt`; the other 1237865950621 ns remain unclassified.
The marked share is a lower bound on runnable delay, not an upper bound or
an estimate of blocked time. An unmarked departure can still leave a thread
runnable, and a sleeping thread can become runnable before it switches in.

A controlled pipe fixture tested scheduler wakeups without changing host
tracing settings (kernel scheduler statistics were disabled). The original
filters used process-visible TIDs and returned no tracepoint samples. An
unfiltered recording attached only to the fixture's reader and waker exposed
the mismatch: the reader's switch-out sample identified process `4139858`, but
the scheduler event payload named TID `156139`. Joining by the mapped target
reproduced the known blocked interval, and a fresh task-scoped probe repeated
the result. Task attachment alone cannot see wakeups from arbitrary wakers.

A small CPU-wide probe instead filtered only for a unique owned reader name
on the two fixture CPUs. It independently replayed a 622071-byte recording
with the reader on CPU 1 and the waker on CPU 0. `sched_waking` ran on CPU 0;
`sched_wakeup` ran on CPU 1 and marked the runnable transition 11887 ns later.
From sleeping switch-out to runnable transition was 35166018 ns; from there
to switch-in was 4259 ns. Deleting the wakeup in a replay failed the join.
These are fixture results, not a split of the NCL1 trace. A representative
capture still needs unique scope for the actual daemon workers, coverage
through worker creation and migration, and loss-free same-window read and
byte reconciliation under a qualified trace budget.

A shared unique name-prefix filter captured two owned threads in the same
process, one created before enable and the other afterward. Both moved CPUs
while blocked. Raw replay joined `1 -> 2` and `2 -> 1` by scheduler TID, with
35236446 and 35193947 ns blocked, and 7036 and 7548 ns runnable, respectively.
Removing the late worker's wakeup failed the replay.

At 56 fixture threads (28 early, 28 late), the same strict split was rejected:
55 complete chains on three CPUs and 53 on all online CPUs. Each incomplete
chain had a sleeping switch-out, wake initiation and switch-in, but no
`sched_wakeup` in the reference window. All-CPU capture did not eliminate the
gap, and there were no reported lost-record markers. A stationary control
also rejected on one inspected chain with two distinct wake initiations and
no runnable-transition event. These probes are not evidence of tracepoint
loss mechanism or backup scheduler timing. Keep the exact gate: when only a
single ordered wake initiation and switch-in are known, the interval between
them can bound the unknown runnable portion, but must not be reported as its
duration or silently counted as blocked time.

A separate diagnostic replay tested this bound against the two known-complete
worker intervals. On masking both `sched_wakeup` records, the measured
runnable times (7036 and 7548 ns) fell inside their respective bounds
`[0, 22894]` and `[0, 26809]` ns. The exact gate still rejected. On the two
incomplete 56-worker traces, summing the independently reconciled intervals
yielded runnable-share bounds of 0.214--0.238% (three CPUs) and 0.034--0.046%
(all online CPUs). The extra uncertainty was 464170 and 238482 ns, respectively.
The stationary trace was rejected even by bounded replay: a duplicate wake
initiation did not identify a unique transition. Missing initiation and a
reported record loss were also rejected. These numbers characterize controlled
pipe waits only. Applying the approach to NCL1 would first need an independent
join to the actual reader syscalls and exact same-window coverage; the earlier
accepted NCL1 trace contains neither wakeup anchor.

The Linux-only diagnostic observer now accepts a per-capture scheduler name
through `VAULTIC_DIAGNOSTIC_SCHED_COMM` (`vlt` plus 12 lowercase hex digits).
Only the existing observed blocking file-body worker attempts to set the
name, at most once per thread. An unset or malformed value and a direct read
do not rename threads. This does not create workers or add reads. A worker
that could not be tagged must fail a future scheduler coverage check rather
than be assumed to have no waits.

The isolated pinned `object_store` build verified the name on the sole Tokio
blocking worker after an encrypted file-body test; the caller stayed untagged.
Paired fixture perf recordings independently found the five-byte `pread64` at
offset two with the diagnostic name enabled and the original Tokio name when
unset. This qualifies worker identification at the syscall, not a joined
scheduler capture. Next, capture the tagged scheduler events and positional
reads on one monotonic clock and check complete identity, window, and byte
reconciliation on a fixture before launching another NCL1 backup.

The dual-recorder preflight then joined four actual five-byte reads from that
encrypted file-body fixture on disposable NASDA2 storage. Both recorders used
`CLOCK_MONOTONIC`; the task-scoped one captured `pread64` and NFS events, and
the CPU-wide one filtered scheduler events for the unique tag only. For each
read, replay checked the offset, count, return value, NFS device, and common
window. The observed sleeping switch-out tied the tagged scheduler payload
TID to the reader sample's PID/TID. The two recording hooks have distinct
timestamps, so the join checks ordering instead of expecting equality.

| Fixture | Syscall, ns | Blocked, ns | Runnable, ns |
| --- | ---: | ---: | ---: |
| First | 514267 | 422225 | 4832 |
| Repeat 1 | 7443 | 0 | 0 |
| Repeat 2 | 404603 | 332378 | 5418 |
| Repeat 3 | 12363 | 0 | 0 |

Independent replay verified all four raw trace pairs. Both blocking reads
switched out in `D` state; neither recording showed an in-read switch for the
other two. Removing the runnable transition from the first trace failed the
join. These results validate an actual-reader scheduler join but do not
estimate the NCL1 split or identify an NFS RPC wait. The representative gate
still requires a multi-interval join at worker-count and event-volume scale,
bounded capture on both streams, and exact same-window read and byte counts.

### Pipeline And RAM Investigation

The November 8 screen completed all 20 legs at
`/volume2/NASDA2/vaultic-test-m6/pipeline-read-fme-cae1-20261108-400k`.
Control and candidate CLIs used identical profiling/build options; the native
daemon retained its qualified bytes. The parent database occupied 41153154586
bytes (38.33 GiB). The fixed schedule was
`TUUT | UVVU | VWWV | WXXW | XYYX`, with two observations per side per bracket.
All legs reused exactly 400000 records, performed no new revision allocation
or publication, and passed restore, encrypted reopen, zero-data-blob and
unchanged-source checks. Main times were 187.459--288.343 seconds, or
1387.24--2133.79 entries/s; backup commands were 192.823--294.227 seconds.
Whole-leg peak combined RSS was 40.625--48.692 GB, below the 200 GB ceiling.

| Change | Control Median, s | Candidate Median, s | Observed Time Ratio |
| --- | ---: | ---: | ---: |
| T to U: directory prefetch at group 16 | 250.811 | 253.428 | 0.990 |
| U to V: publication group 64 | 261.919 | 277.284 | 0.945 |
| V to W: one-batch collection read-ahead | 284.850 | 224.180 | 1.271 |
| W to X: collection batch 5000 to 10000 | 223.198 | 228.724 | 0.976 |
| X to Y: RAM-heavy native and CLI caches | 206.885 | 226.438 | 0.914 |

Directory prefetch reduced each pointer/revision call count from about 66689
to 25047; group 64 reduced it further to about 6300. Neither reduction alone
improved whole-backup time in its bracket. Read-ahead reduced writer wait for
prepared work from a median 183.781 to 79.947 seconds, while overlapping batch
work increased from 83.655 to 126.851 seconds. These are overlapping phases,
not additive worker CPU measurements. Keep batch 5000 for the next screen;
batch 10000 did not improve the observed result.

Y used native metadata/block caches of 8/8 GiB, authenticated-chunk retention
of 4 GiB, and an 8 GiB CLI on-demand cache. Its exact-main logical ciphertext
origin bytes fell from 49.936 to 41.493 GB (16.91%), with approximately 7.10 GB
more median whole-leg peak RSS. This is useful work reduction, not a proven
throughput regression or gain: the final X control had 140.764 accumulated
origin-worker seconds versus 1658.936 in the preceding X control, despite
nearly identical logical read volume. Host cache and origin service varied;
two observations per side cannot settle a marginal RAM benefit.

All 20 matched CLI CPU profiles decoded. They cover the whole backup command,
not the exact main interval or native-daemon CPU. Representative W/Y profiles
attribute roughly 28--29% cumulative sampled CPU to the file saver, 18--19%
to chunk splitting, 15% to SHA-256, and 18% flat to syscalls. Cumulative paths
overlap. These forced backups still read/chunk/hash source data even though
they add no data blobs. Metadata RPC counts alone therefore cannot predict
end-to-end throughput, and no source-read-byte counter was collected.

The CLI lookup cache had zero evictions and a peak 213113 handles: the 64 MiB
budget already allows 349525 accounted entries. An 8 GiB budget increases
capacity to 44739242 handles but does not reduce a currently absent eviction
cost. For planning only, twice this observed handle set needs about 78 MiB
and ten times needs about 390 MiB at 192 accounted bytes/entry; 128/512 MiB
are corresponding rounded budgets, not future performance qualifications.
Native metadata/filter/index and block caches have different working sets;
do not size them by total database bytes or conflate them with this CLI cache.

Independent post-run validation replayed all 20 rows and report calculations,
verified 620 leg checksums, 63 series checksums, 21 bundle checksums and all
12 launch pins. No owned executable processes or private runtimes remained.
Private decoded profiles and verification are retained in the sibling
`pipeline-read-fme-cae1-20261108-400k-analysis` directory. The captures remain
sealed and unchanged. No production defaults were changed. Causal speedup,
physical NFS traffic, sustained 600-second operation and larger-database
growth remain unqualified; proportional cache estimates are not a substitute
for a representative retained-history/growing-live-set experiment.

#### Larger Opt-In Chunk Cache Follow-Up

The existing 4 GiB/16384-entry ceilings prevented testing substantially more
RAM. The explicit supported ceilings are now 16 GiB/65536 entries, with the
64 MiB/4096-entry defaults unchanged. All 44 encryption/cache tests, including
offline cases, passed. Authentication, invalidation, per-cache byte accounting,
shared budgets, single-flight limits and strict input bounds remain intact.

The follow-up uses fresh native processes and one pinned candidate CLI/native
pair for all eight legs: `Z AA AA Z | AA Z Z AA` (ABBA then BAAB, four
observations per capacity). Z retains 4 GiB and AA 16 GiB; both allow 65536
entries. Group 64, read-ahead 1, collection batch 5000, native metadata 2 GiB,
block 512 MiB, CLI cache 64 MiB, 128 readers, RPC limit 32, eight crypto workers
and existing fetch/range limits are fixed. The create-only root is
`/volume2/NASDA2/vaultic-test-m6/ram16-read-fme-cae1-20261109-400k`.
The 600-second backup-command deadline and all ownership, source, full-reuse,
restore/reopen, read-only isolation, RSS and storage gates are unchanged.
Status: all eight legs completed and were independently accepted. Each reused
all 400000 records with no new allocation/publication and passed full restore,
encrypted reopen, source identity and zero-data-blob checks. Main times were
155.886--203.689 seconds (1963.78--2565.97 entries/s); backup commands took
161.125--209.304 seconds, with no deadline or cleanup fallback failure.

| Measurement | Z: 4 GiB Median | AA: 16 GiB Median |
| --- | ---: | ---: |
| Main seconds | 191.695 | 171.083 |
| Backup-command seconds | 197.056 | 176.713 |
| Exact-main logical ciphertext origin, GB | 41.697 | 25.850 |
| Exact-main origin calls | 156812 | 96301 |
| Exact-main authenticated-chunk hits / misses | 44574 / 155749 | 104938 / 95304 |
| Exact-main authenticated-chunk evictions | 139577 | 29981 |
| Exact-main crypto service, accumulated worker-seconds | 24.433 | 14.984 |
| Whole-leg peak combined RSS, GB | 47.602 | 61.705 |

The overall median-time ratio is 1.120: approximately 12.0% higher observed
throughput, or 10.8% less main time. Both orderings favored 16 GiB: the ABBA
bracket was 195.491 versus 183.477 seconds (ratio 1.065), and BAAB was
189.360 versus 160.112 seconds (ratio 1.183). Logical ciphertext reads fell
38.0%, origin calls 38.6%, and crypto-service worker time about 38.7%.
Additional median whole-leg peak RSS was 14.103 GB, below the 200 GB guard.
Logical read reduction is not physical NFS traffic measurement. Origin-worker
times varied substantially, so the balanced short screen supports the RAM
tradeoff but does not establish a causal speedup or an optimum cache size.

All eight matched CLI profiles decoded. Representative adjacent BAAB profiles
had nearly equal sampled CPU totals (477.63 and 478.64 seconds), with source
chunking, hashing, syscalls and path handling still prominent. This is not a
claim about exact-main or native CPU: the profiles cover whole backup commands.
Extra chunk retention reduces authenticated-read work, not the forced source
read/hash requirement. Increasing the unpressured CLI lookup cache is not a
substitute for authenticated-chunk retention.

Independent verification replayed every row and report calculation, verified
248 leg checksums, 27 series checksums, 18 bundle checksums, two prelaunch
checksums, all 13 launch pins and seven build-source pins before updating this
document. No owned processes or any of the eight private runtimes remained.
Raw profiles, decoded profiles, measurements and verification are retained in
the private sibling `ram16-read-fme-cae1-20261109-400k-analysis`; original
captures and build artifacts remain sealed and unchanged.

#### Recommended Qualification Profile And Growth Gate

Given the preference for RAM over marginal throughput loss, use 16 GiB
authenticated-chunk retention with a 65536-entry ceiling for the next dedicated
qualification. Keep group 64, one-batch read-ahead and collection batch 5000;
the 10000-entry batch and broad 8 GiB CLI/metadata/block increases did not help
in their brackets. Retain metadata 2 GiB, block 512 MiB and CLI lookup 64 MiB
until their own occupancy/eviction evidence warrants growth. The opt-in chunk
settings are `VAULTICDB_CHUNK_CACHE_BYTES=17179869184` and
`VAULTICDB_CHUNK_CACHE_ENTRIES=65536`. This recommendation is specific to the
tested dedicated-force/immutable benchmark case; it does not authorize changing
production backend/freshness policies or bypassing their qualification.

The combined implementation and 28 measured legs are complete, but the
database remained 38.33 GiB with 400000 source entries. Substantial retained
history and a larger changing live set were not generated. Larger-database
performance therefore remains explicitly unqualified. A representative growth
gate must measure both history-heavy and live-set-heavy databases, retaining
exact authenticated read/RPC/cache occupancy, command deadlines, correctness
and combined-RSS evidence under the same workload/order controls. Any increase
to the explicit source/fixture/entry limits requires a separately bounded
qualification; do not pad short runs or spoof filesystem identity. Budget
memory for actual hot chunks, lookup handles, live publication working state
and concurrent operations, rather than total database bytes alone. The current
16 GiB ceiling is neither a promise that future working sets fit nor proof
that a larger cache will be unnecessary. No production defaults, deployment,
format, or filesystem identity were changed by this investigation.

### Immutable Dedicated-Force Baseline

The new default benchmark case is explicitly
`VAULTICDB_BACKEND_ACCESS_MODE=dedicated-force` plus
`VAULTICDB_SST_FRESHNESS=immutable`. This is a benchmark baseline, not a change
to production settings or the application's shared/bounded defaults.

Three forced 200k observations completed with fresh daemon processes, fixed
128 readers / 32 metadata RPCs / publication group 16, 2 GiB metadata cache,
512 MiB block cache, eight crypto workers, 1ms batching and 64 MiB authenticated
chunk retention. Exact restore, encrypted reopen, source identity, zero added
data blobs, idle-writer and live-policy checks passed for all three.

| Measurement | Run 1 | Run 2 | Run 3 |
| --- | ---: | ---: | ---: |
| Main seconds | 201.762 | 192.574 | 187.800 |
| Entries/second | 991.3 | 1038.6 | 1065.0 |
| Command seconds | 206.995 | 197.505 | 192.673 |
| Peak combined owned RSS, GB | 41.039 | 40.876 | 41.105 |
| Writer batch seconds | 103.436 | 101.712 | 101.327 |
| Prepared-work wait seconds | 87.708 | 79.781 | 76.269 |
| Existing-record lookup seconds | 39.166 | 40.449 | 37.621 |
| Main header validations | 0 | 0 | 0 |
| Main logical origin ciphertext, GB | 29.275 | 29.281 | 29.276 |
| Main origin/requested-plaintext ratio | 22.063 | 22.066 | 22.064 |
| Main origin accumulated worker-seconds | 462.913 | 195.960 | 115.949 |
| Main crypto queue / service worker-seconds | 2.825 / 16.714 | 3.033 / 16.310 | 3.243 / 15.901 |
| Size-RPC peak active / cap | 24 / 32 | 24 / 32 | 20 / 32 |

Median main time is 192.574 seconds (1038.6 entries/second). Header freshness
validation is no longer a main-phase cost. The wider before/after writer-status
window includes correctness work: it reports one header validation, approximately
29.46 GB origin bytes and 40k chunk hits per run. Do not mix those counters with
the main-phase counters above, where chunk hit rate is approximately 16.5%.

Remaining investigation targets, in priority order:

- Reconciliation's read/reuse path: writer batch processing is 51.3--54.0% of
   main time, with 37.6--40.4 seconds in existing-record lookup. All 200k records
   are reused. Publication-group and lookup timers overlap; they are not extra
   wall-time components. Prepared-work wait is another 40.6--43.5%, including
   batch collection, and does not imply continuous source-stat starvation.
- Logical read amplification/locality: about 109.4k origin reads fetch 29.28 GB
   for 1.327 GB of requested plaintext. Main chunk misses are approximately
   108.4k, with approximately 108.2k evictions. Cold random block misses remain
   expected; counts alone are not a defect. Profile the lookup path and consider
   controlled smaller encrypted-read granularity or locality experiments before
   assuming a larger cache is the answer.
- MultiGet request work: 255.8--258.0k calls consume 441.8--596.7 accumulated
   client worker-seconds, predominantly server storage work. Outside-handler
   time is 77.9--80.0 worker-seconds, not pure network latency. Size-RPC admission
   wait is only 0.146--0.200 worker-seconds, so this does not justify raising the
   RPC cap. Durable waits total 0.968--0.997 worker-seconds, engine backpressure
   is zero, and crypto queue/service totals do not indicate crypto saturation.

Native CPU averages 1.44--1.47 cores within the first-to-last sampled backup CLI
lifetime; CLI command CPU averages 1.77--1.82 cores. Native CPU windows are
191--205 seconds and are not exact main-phase bounds. Shared host CPU pressure
was negligible, but aggregate low CPU use cannot exclude serialization on a
particular thread or lock. The observer's automatic active-stage labels are
empty for this runner; PID/lifetime bounds were used instead. Worker timers
overlap and logical origin bytes are not physical NFS traffic.

Evidence is sealed at
`/volume2/NASDA2/vaultic-test-m6/immutable-default-fme-cae1-20261105-200k-retry1`:
84 leg checksums, 12 series checksums and all 11 launch pins were independently
verified. No owned survivors or private runtimes remained. Derived analysis is
the separate sibling `immutable-default-fme-cae1-20261105-200k-retry1-bottleneck-analysis.json`
(SHA-256 `8ba86165f5cdfb4d3093ee8a3b0619ed194ab7c7996d3eefbe8faa6bdd2d193f`).

The retained parent uses a separate WAL handoff, which dedicated-force rejects.
Only disposable clones were converted to inherited WAL: regular WAL objects
retain their logical paths and exact ciphertext, differing collisions reject
the fixture, and a per-object hash receipt is retained. The immutable parent
and production data were not modified. The first attempt failed a harness-only
runtime-prefix check before daemon startup; its 19-artifact failed seal and
original binaries are preserved separately without the `-retry1` suffix.

These are three sequential baseline observations, not a paired causal speedup
comparison. Host-cache effects are uncontrolled and WAL topology also differs
from historical captures. The 146k-per-leg validations in the earlier sealed
chunk screen versus one in the wider new window establish a work-count change,
not an isolated throughput gain. All main intervals remain below 600 seconds;
sustained qualification and production deployment are not established. No NFS
tuning, cache flushing or writer-concurrency defaults were changed.

### Forced-Only Concurrency Screen

The follow-up optimization scope is forced backups only; unchanged-path work is
deferred. The initial mixed-mode concurrency sequence was interrupted via its
owned supervisor before any measured backup began. Its failed/aborted evidence
is preserved and is not a performance result.

The new screen reuses the exact frozen 50k input and verified parent from the
fivefold observation, rather than selecting a different source or rebuilding
the comparison binaries. Each cell gets an independent checksum-verified
repository/database clone, a fresh encrypted daemon and one forced full crawl.

| Profile | Content Readers | Metadata RPC Limit |
| --- | ---: | ---: |
| baseline | 16 | 8 |
| rpcs | 16 | 32 |
| readers | 64 | 32 |

Caches remain 128 MiB metadata / 512 MiB block, engine MultiGet stays enabled,
batch delay stays 1ms, and publication stays at group size 64. Unchanged
validator settings are not varied: forced files use the content-reader pool.
Fresh evidence roots append `-forced-concurrency-{profile}` to the 50k root.
The supervisor requires a forced-only schedule and rejects an unforced leg.
Existing ownership, read-only protected mounts, RSS, disk-floor, fixture,
timeout, SIGINT-first cleanup and exact restore/reopen/source checks remain.

Status: completed; all three forced-only correctness captures accepted. The
create-only analyzer verified the launch pins, fixed settings, source/parent and
force semantics, then sealed 27/25/25 files. All 77 root-relative checksums were
independently verified, and no candidate daemon survivors remained. Every cell
passed exact restore, encrypted reopen, source identity, idle-writer checks and
zero new data blobs. Peak combined owned RSS was 41.200/42.618/44.207 GB.

| Measurement | baseline 16/8 | rpcs 16/32 | readers 64/32 |
| --- | ---: | ---: | ---: |
| Main / command seconds | 105.077 / 125.392 | 96.424 / 116.856 | 79.823 / 98.967 |
| Entries/second | 475.841 | 518.542 | 626.388 |
| Throughput relative to baseline | 1.000 | 1.090 | 1.316 |
| Paired storage mean ms | 49.954 | 58.403 | 45.349 |
| Actual size RPC peak / limit | 8 / 8 | 32 / 32 | 26 / 32 |
| Handles/size RPC | 4.317 | 3.740 | 4.932 |
| Size slot wait worker-seconds | 371.534 | 0.633 | 0.032 |
| Crypto queue / service worker-seconds | 505.310 / 269.878 | 1094.242 / 314.205 | 1019.778 / 280.205 |
| Ciphertext-origin worker-seconds | 1223.814 | 1209.456 | 623.511 |
| Logical metadata GB | 547.026 | 532.757 | 488.931 |
| Prepared-work wait seconds / main share | 66.051 / 62.9% | 56.247 / 58.3% | 41.643 / 52.2% |
| Writer batch seconds | 33.195 | 34.454 | 32.175 |

Raising the RPC cap alone improved throughput 9.0% and removed admission waiting,
but increased mean storage latency and more than doubled crypto queue work.
More RPC overlap therefore helped while exposing an inner queue, not eliminating
latency. Raising content readers at the same 32-RPC cap improved throughput a
further 20.8%, improved batching and reduced measured logical metadata work.
The best cell was 31.6% faster in throughput than baseline, with 24.0% less main
time and 10.6% less logical metadata work. These are observations from one
sequential screen; host/NFS warming and timing-dependent grouping remain
confounders, so they do not qualify a causal amplification or physical-I/O gain.

At 64 readers the RPC peak was only 26 of 32 and admission waiting was negligible.
There is no evidence here to raise that cap further. The writer still waits for
prepared work for 52.2% of main time, while publication occupies 32.175 seconds,
about 40.3%. The next forced-path discriminating screens should hold readers/RPCs
fixed at 64/32 and separately vary effective engine caches and crypto workers.
`CryptoExecutor` uses a bounded dedicated pool with a matching semaphore;
`VAULTICDB_CRYPTO_THREADS` is its existing startup control. Its queue timer
includes permit waiting and pool scheduling, so this evidence suggests testing
worker capacity, not asserting that decrypt service alone is the bottleneck.
Batching/cache reuse can instead reduce the repeated work submitted to that pool.

The immutable baseline `verified-concurrency.json` and each cell's
`CONCURRENCY-SHA256SUMS` retain the measurements. Do not rerun the create-only
analyzer or overwrite sealed receipts. Worker-second sums overlap and cannot be
added as wall-time shares; logical/origin bytes are not physical NFS/HDD traffic,
and full SST decoding is not isolated. No main interval reached 600 seconds.
This screen is diagnostic, not repeated mirrored/sustained qualification or a
default/deployment change. Unforced optimization remains deferred.

### Fivefold Inode Observation

At the user's request, the next coalesced observation selects exactly 50,000
entries, five times the 10k diagnostic scope, from the accepted 100k restore.
It creates a fresh stable source and a complete verified parent before cloning
independent unchanged and forced legs. This is not permission to modify the
production source, repository, daemon, or earlier sealed captures.

The comparison CLI and native binaries remain byte-identical to the 10k
candidate. Validators 16, metadata RPCs 8, batch delay 1ms, engine MultiGet on,
metadata/block caches 128/512 MiB, content readers 16 and publication groups 64
remain fixed. The source mix and inode paths are a larger fresh selection, not
an interleaved same-input performance comparison. Report per-entry work as well
as totals, and do not assume total runtime must increase exactly fivefold.

The expanded supervisor uses `--mode coalesced --inode-limit 50000`. Its fresh
evidence root is
`/volume2/NASDA2/vaultic-test-m6/metadata-read-fme-cae1-20261103-50k-coalesced`;
runtime state is private under `/run`. Durable fixture storage is explicitly
bounded at 350 GiB, allowing the verified parent and cloned packs/databases plus
three verified restores. RSS, managed RAM, 2 GiB scratch, shared free-space and
time ceilings remain unchanged. Parent setup, clones and verification stay
outside measured main intervals. A main interval shorter than ten minutes must
still be labelled a short screen, without padding or extrapolation.

Status: completed and correctness accepted. Both measured legs covered exactly
50,000 entries, with 41,347 regular files and 19,493,496,103 payload bytes. The
source hash was
`8f32883b02c37ae550a0375fa6abfe79ec52d830ef0d9cff1053522687a41e87`.
Both stored zero new data blobs and passed exact restore, encrypted reopen,
source identity, expected force/parent semantics and idle-writer checks. Outer
cleanup passed; a subsequent check found no candidate daemon survivors. All
11 launch pins and 46 root-relative evidence checksums independently verified.

| Measurement | Unforced 50k | Forced 50k |
| --- | ---: | ---: |
| Main / command seconds | 316.380 / 335.310 | 105.016 / 122.747 |
| Entries/second | 158.038 | 476.119 |
| Prepared-work wait seconds / main share | 264.427 / 83.6% | 64.737 / 61.6% |
| Writer batch seconds | 46.203 | 34.560 |
| Size RPCs / handles | 32936 / 41372 | 9857 / 41372 |
| Handles/size RPC | 1.256 | 4.197 |
| Paired blob-read client / storage mean ms | 19.454 / 18.749 | 48.448 / 47.559 |
| Size slot / result wait worker-seconds | 0.220 / 749.318 | 390.108 / 3337.359 |
| Ciphertext-origin worker-seconds | 642.349 | 1115.296 |
| Crypto queue / service worker-seconds | 78.906 / 379.820 | 525.201 / 267.601 |
| Logical metadata GB | 932.811 | 545.185 |
| Logical metadata MB/entry | 18.656 | 10.904 |

The dependency remains writer waiting for prepared nodes, with the unchanged
availability lookup mostly waiting for the database response rather than RPC
admission. Its paired mean is 96.4% inside storage, not transport alone. Size
RPCs peaked at eight in both legs; a peak is not evidence of continuous
saturation or an engine concurrency knee. Unchanged batches remain close to
singletons despite 16 validators and the 1ms coalescing window.

The forced pipeline exposes a different pressure pattern: RPC-slot waiting is
substantial, paired reads are 98.2% inside storage, and origin fetching plus
crypto queueing dominate the measured encrypted-read work. Writer publication
also costs 34.560 seconds, but prepared-work waiting remains larger. These
worker-second totals overlap across concurrent requests; they cannot be added
as exclusive wall-time shares. Origin work can be encrypted-cache-served, and
neither its bytes nor logical wrapper bytes measure physical NFS/HDD traffic.
CLI size-cache and encryption-header-cache evictions were zero; this does not
establish that decoded engine metadata or block caches fit the working set.

Relative to the 10k coalesced screen, unforced main time grew 4.555-fold while
throughput rose 9.8%; forced main time grew 5.784-fold while throughput fell
13.5%. Logical metadata work per entry fell 9.1% unforced and rose 3.1% forced.
These are descriptive scope contrasts, not isolated optimization gains: payload
grew 7.9-fold and source mix/private paths differ. Weak unchanged batching and
continued amplified metadata work are still the controlling issues; no higher
defaults or production activation follow from this observation. Separate-variable
decoded-cache and batching screens remain the next amplification experiments.

Peak combined owned RSS was 42,161,598,464 bytes. Outer capture time was
5,682.491 seconds, including source preparation, parent setup, clones and
verification. **Neither measured main interval naturally lasted ten minutes**;
the larger observation still does not satisfy repeated sustained qualification.
The immutable `verified-observation.json` and `METADATA-READ-SHA256SUMS` in the
50k evidence root retain the measurements and explicit limits. Do not rerun the
create-only analyzer or rewrite the sealed receipt.

## NASDA2 Concurrency Headroom

On 2026-11-18, two six-second passes covered hot reads, wide reads, writes,
and mixed traffic at each of depths 2, 8, 24, 56, and 96. Six additional
30-second holds compared depth 24 with a selected depth for each of wide
reads, writes, and mixed traffic. Direct, 262,160-byte reads matched the
dominant request size of the accepted backup trace and used 170 read-only
metadata objects totaling 38.27 GiB. One-MiB writes used fresh private files,
with per-file `fdatasync`, tail readback and cleanup. Mixed legs allocated
half the requested workers to each direction. Direct I/O bypassed the client
page cache, not necessarily NASDA2's cache.

| Workload | Requested Depth | Effective Read / Write Depth | Read / Write MiB/s | Read / Write p99 ms |
| --- | ---: | ---: | ---: | ---: |
| Wide | 24 | 24.0 / - | 356 / - | 89.9 / - |
| Wide | 96 | 95.7 / - | 511 / - | 217.6 / - |
| Write | 24 | - / 23.5 | - / 1,045 | - / 31.7 |
| Write | 56 | - / 55.4 | - / 952 | - / 71.0 |
| Mixed | 24 | 12.0 / 11.6 | 95 / 990 | 115.6 / 16.3 |
| Mixed | 56 | 27.9 / 27.6 | 163 / 726 | 143.9 / 81.3 |

The wide-read holds delivered 43% more completions at higher depth despite
higher latency. Client READ RPC queueing stayed below 0.05 ms on average;
mean RTT rose from 14.0 to 44.6 ms. Hot reads already reached 1,020-1,104 MiB/s
at depth 8, close to the observed ceiling on the 10 GbE client path. Cache
order mattered: wide reads at depth 2 delivered 57 and 776 MiB/s in the two
passes. These are client-observed results, not cold-media measurements or
proof of HDD seek scheduling. No NAS-side disk, cache, or server queue
telemetry was collected.

Write-only holds saw one NFS COMMIT per completed application write and about
1.4 ms of final drain. Mean client WRITE RPC queue time rose from 13.9 to
49.1 ms while RTT stayed near 4 ms. Higher depth reduced write completions in
the longer hold. Mixed results varied with run order and the direction being
measured; sizing by aggregate MiB/s hides that tradeoff. Do not treat
`fdatasync` and NFS COMMIT as a physical-media power-loss test.

The accepted unchanged backup read directly from `/ncl1-1-vs-50/fme_cae1`.
Exact replay of its 152,317 matched NASDA2 reads measured a mean depth of 4.25,
depth zero for 48.3% of the 300.15-second interval from first matched entry
to last matched completion, and a brief peak of 90. Its 32-slot lookup limiter
recorded 0.481 aggregate seconds of admission wait across 83,378 size RPCs.
That does not justify a 32-to-64 RPC-cap backup trial. Keep existing caps;
test a bounded source of independent native reads that can fill idle periods
instead of enlarging bursts, and vary source traversal separately. This sweep
did not establish NCL1 source saturation or qualify a changed-data backup.

The exact trace contained 144.903 seconds with no matched read outstanding.
Contiguous gaps of 1-10 ms accounted for 46.941 seconds (14,060 gaps), and
10-100 ms gaps accounted for 44.342 seconds (1,660 gaps). Another 26.224
seconds fell between 100 ms and 1 second; five gaps of at least one second
totaled 24.085 seconds, including the 17.838-second maximum. Gaps shorter
than 1 ms accounted for the remaining 3.311 seconds. These are gaps in the
allowlisted reads, not proof that the daemon or NAS was idle.

At the locked SlateDB revision, multi-get already fans out planned SST reads
up to 32 at a time and restores SST rank before resolving versions. The
encrypted store also joins the chunks of an eligible range concurrently.
Before adding another fan-out layer, collect same-clock samples of lookup
batch readiness, native read admission, and matched reads in flight. The
retained reconciliation progress has no shared monotonic timestamps with the
read trace, so it cannot identify ready work during each gap. A useful
candidate must reduce gaps with ready work without adding origin bytes or
relaxing version and authentication checks.

The opt-in Go runtime trace now marks size-lookup slot wait, batches ready
after local cache and deduplication checks, and actual RPC start and finish
after coalescing. A batch-ready event is not a daemon read. A traced fixture
emitted 32 ready batches and two RPCs; the authenticated native lookup emitted
one ready batch and one RPC. Perf now requests `CLOCK_MONOTONIC` explicitly.
In two positive recorder smokes, the unique kernel `pread64` entry and exit
fell between the Go trace markers bracketing that same read. This validates
the clock join on this host, not production timing. The CLI trace was then
capped at 512 MiB with a completion receipt; the native observer was capped
at 16 MiB and records submission time, worker start time and Linux thread
ID. An encrypted fixture validated this boundary, and a separate exact
five-byte perf join measured 154,663 ns from submission to worker start
and 79,913 ns from worker start to kernel entry. Those are fixture timings,
not estimates of admission cost in a representative backup.

The first same-clock 800k route used the direct NCL1 source and finished
the backup command in 303.683 seconds (296.602 seconds main), with zero new
data blobs. The command-window gate and perf replay matched 152,312 reads
and 40,755,813,940 bytes exactly. The later status sample, taken after a
snapshot lookup, failed the origin request/body drain check. Its counters
were not preserved and the native admission file was not retained; neither
restore nor encrypted reopen ran. This is a rejected qualification, not a
joined admission result or a speed comparison. The retry uses the strictly
checked command endpoint for attribution, retains the separate post-lookup
status, and copies the native stream before snapshot lookup. The rejected
evidence is sealed under `ncl1-admission-fme-cae1-20261118-800k` in the same
NASDA2 evidence directory.

Retry 1 finished the direct-source backup in 308.920 seconds (301.882 seconds
main), with zero new data blobs. Its strict command-window replay matched
152,303 reads and 40,753,454,500 bytes. Qualification stopped before snapshot
lookup, restore, or reopen: the Go daemon launcher filtered the ambient native
admission path, so the file never existed. The retry evidence is separately
sealed under `ncl1-admission-fme-cae1-20261118-800k-retry1`; it does not support
an admission or throughput conclusion. Activation now uses an explicit test
option on the measured child only, with an artifact check before backup.
An encrypted launch and reopen using the same pinned native binary produced
nine valid admission records; the uninstrumented launch produced none.

Retry 2 finished the direct-source backup in 300.124 seconds (292.992 seconds
main), with zero new data blobs. Snapshot validation, verified restore,
encrypted reopen, and source digest comparison all passed. All three
traces were retained, but strict perf replay rejected an unpaired scheduler
edge inside a positional syscall. The sealed series under
`ncl1-admission-fme-cae1-20261118-800k-retry2` remains rejected for scheduler
attribution. A separate positional-only replay verified 152,346 syscall
pairs and matched 152,305 reads and 40,753,978,820 bytes exactly against
the authenticated command window. It does not report off-CPU timing or
change the rejection.

The first joined replay hit the Go trace decoder's time limit while formatting
the full trace. A selective extractor using the same installed Go trace
reader decoded to EOF and matched all 85,985 reported size RPCs. It joined
152,096 reads to unique same-thread native starts, excluding 209 first reads
on a thread from admission timing because startup records precede perf.
Native submission-to-start p99 for joined reads was 97.9 microseconds.
There were 145.561 seconds at zero matched depth over a 298.821-second span.
Of those gap seconds, 1.924 overlapped native queued reads, 0.318 overlapped
a CLI lookup-slot wait, and 68.137 overlapped an in-flight size RPC. These
intervals can overlap one another and cover only the recorded stages. They
do not establish the cause of the gaps or imply that NASDA2 was idle.
Keep the current limits; the measured queue overlaps do not support raising
the CLI slot cap or native blocking pool as the first experiment.

The sealed retry 2 replay also split the 25,960 gaps by duration. Times below
are mutually exclusive: an in-flight size RPC takes precedence; queue/slot
time is counted only outside RPCs; the remainder is unclassified. A ready
event is a point, not a measured wait duration.

| Gap Duration | Gaps | Total Seconds | RPC Seconds | Queue/Slot Outside RPC Seconds | Unclassified Seconds |
| --- | ---: | ---: | ---: | ---: | ---: |
| Under 1 ms | 9,650 | 3.579 | 3.578 | <0.001 | 0.001 |
| 1 to under 10 ms | 14,457 | 47.242 | 42.923 | 0.039 | 4.280 |
| 10 to under 100 ms | 1,716 | 44.590 | 18.243 | 0.046 | 26.301 |
| 100 ms to under 1 s | 132 | 26.849 | 3.337 | 0.008 | 23.505 |
| At least 1 s | 5 | 23.302 | 0.058 | <0.001 | 23.244 |
| Total | 25,960 | 145.561 | 68.137 | 0.093 | 77.330 |

3,536 gaps contained a ready event outside any size RPC; 668 contained
neither an in-flight RPC nor a ready event. The longest gap (17.173 s) fell
in the latter group. This points to different measurements for short gaps
inside RPCs and longer gaps outside them. It does not show whether an
emitted batch remained pending or whether the NAS was idle.

The backup now emits opt-in Go trace regions for the progress source scan,
the enclosing archiver snapshot call, the authoritative reconciliation drain,
and the final scope publication. Small ordinary and authoritative backup and
restore tests emitted balanced pairs for all four. The scan is not the whole
archiver traversal, the drain is not the whole reconciliation pipeline, and
the regions can overlap. Use them to narrow the long-gap boundary, then add
markers inside the owning phase only as needed. For short gaps inside RPCs,
the next refinement is daemon receipt, read-plan completion, and first
submission. Do not select a throughput candidate solely from overlaps;
compare a concrete change only after verifying the affected phase on the
direct NCL1 source with the same parent and disposable NASDA2 output.

The next direct-source phase capture passed backup, verified restore,
encrypted reopen, source-integrity checks, and exact positional replay:
152,309 matched reads and 40,755,027,460 bytes. It took 494.535 seconds main
and 501.881 seconds for the command. This is a diagnostic leg, not a paired
performance comparison. Scheduler attribution was not attempted.

The first phase replay incorrectly required the optional progress scan even
though the command used `--no-scan`. Replaying the same sealed artifacts with
that expectation corrected required one balanced snapshot, drain, and
publication region, and matched all 116,187 reported size RPCs. The 33,714
gaps totaled 331.972 seconds over a 500.551-second span. Of that gap time,
119.091 seconds overlapped size RPCs and 212.707 seconds were unclassified
after accounting for RPCs and recorded queues. The longest gap (17.717 s)
overlapped the reconciliation drain for 17.512 seconds and final scope
publication for 0.099 seconds. Drain accounted for 20.952 of the 38.900 gap
seconds in gaps at least one second long; other multi-second gaps occurred
inside the snapshot but outside the drain. These are phase overlaps, not
exclusive costs or a storage saturation result. Inspect the work inside the
reconciliation drain before changing concurrency to address the longest gap.

The retained 480-second writer sample was in `publish_directories` and had
been there for 17.621 seconds. Final writer counters recorded 30.056 seconds
in that phase, 1.873 seconds in root finalization, and 0.213 seconds in
hardlink finalization. They also recorded 799,076 reused records, zero
changed records, and zero directory publication calls. The directory phase
name therefore does not imply a write bottleneck. There are no same-clock
subphase boundaries for it in that capture.

Directory finalization now emits Go trace regions around the full method,
grouped existing-record prefetch, and worker dispatch through join. The last
region includes reuse checks and any writes; it is not a storage-write
timer. Directory index, independent sibling overlap, and cancellation
fixtures emitted balanced regions. In a future capture, compare the longest
drain gap with these intervals; time outside prefetch and workers is not
automatically attributable to preparation or encoding.

The 2026-11-20 directory capture passed backup, verified restore, encrypted
reopen, source integrity, and both evidence seals. It ran directly on NCL1
with unchanged settings: 288.185 seconds main, 295.483 seconds command,
152,311 exactly matched reads and 40,755,551,780 bytes, and all 81,613
reported size RPCs reconciled. The writer recorded 31.164 seconds in
directory finalization, 799,076 reused records, zero changes, and zero
directory publication calls. This was not a paired timing comparison.

Of the longest 18.994-second gap, 18.792 seconds overlapped reconciliation
drain and 16.774 overlapped directory finalization. Within the directory
overlap, 8.961 seconds fell inside grouped prefetch, 0.400 inside worker
dispatch through join, and 7.414 outside both regions. Across all gaps,
directory finalization overlapped 30.877 seconds, of which 15.389 were
prefetch and 0.631 were workers. The residual is not measured preparation
time. There was no matched read in flight in these gap intervals, but
other operations could be active. The grouped prefetch currently batches
current pointers first and then batches dependent revision lookups; split
those two waits and measure child indexing, sorting, and record encoding
before changing group width. Increasing directory worker concurrency is
not the first candidate. The sealed series is
`ncl1-admission-fme-cae1-20261120-800k-directories`.

A read-only replay of the retained Go trace accounted for all 16.774 seconds
of directory-writer overlap in that longest gap. Inside prefetch, the writer
spent 3.965 seconds blocked at the current-pointer lookup's `select` and
4.132 seconds at the dependent revision lookup's `select`. These are Go
caller waits, not server service times. Prefetch also included 0.782 seconds
in the Go `Running` state and 0.081 seconds runnable. Outside prefetch and
workers, the writer was `Running` for 7.409 of the 7.414 seconds. `Running`
is a Go scheduler state, not measured CPU time, and the retained markers do
not split this interval by operation.

The follow-up markers bracket those two lookup calls separately, and split
out child indexing, directory sorting, directory record assembly, and
encoding. Assembly can also include cross-filesystem debt handling; it is
not an exclusive CPU timer. The traced fixture exercised actual encrypted
daemon publication and reopen, unchanged reuse without new commits, sibling
publication, and cancellation during child indexing. It emitted balanced
marker pairs. No new representative backup or concurrency change was made.

The subsequent `ncl1-admission-fme-cae1-20261120-800k-dir-split` capture
used the direct NCL1 source and the same controls. Both seals, verified
restore, encrypted reopen, and source integrity passed. Replay exactly
matched 152,310 reads and 40,755,289,620 bytes and reconciled 84,635 size
RPCs. Main and command elapsed were 282.358 and 289.773 seconds. The
writer reused 799,076 records, changed none, and made no directory
publication calls. This unpaired run does not establish a speedup.

In the longest gap (18.894 seconds), directory finalization overlapped
16.681 seconds. Prefetch overlapped 8.691 seconds, including 4.109 seconds
in current-pointer lookup and 4.377 in dependent revision lookup. Assembly
overlapped 0.749 seconds, encoding 0.715, and workers 0.377; indexing and
sorting did not overlap this gap. The disjoint markers leave 6.148 seconds
unattributed within directory finalization. Sorting was the largest marked
directory operation in the second-longest gap (1.170 of 2.399 seconds),
with another 0.532 seconds in child indexing. Across all gaps, directory
finalization overlapped 30.661 seconds, including 15.028 in prefetch and
11.153 outside the marked subregions. These are elapsed overlaps, not CPU
or storage service time.

Keep worker and RPC limits fixed. Before another representative run, use
a bounded unchanged-directory fixture to test whether group-compatibility
checks account for the unmarked time. The pending-group loop recomputes
depth and normalized paths for pairs of entries; measuring that loop
separately would distinguish it from group setup and result bookkeeping.

The bounded unchanged-directory fixture exercised 4,096 siblings and their
parent against an in-memory store, excluding the initial seed from timing.
At group width 64, the baseline traced medians were 295.6 ms for directory
finalization, 228.9 ms for compatibility checks, 2.1 ms for group setup, and
4.4 ms for result handling. The candidate computes cleaned depth and
normalized snapshot path once per directory and leaves the conflict checks
and flush boundaries unchanged.

In eight untraced trials (ABBA then BAAB, five passes per trial), median
width-64 elapsed fell from 281.4 to 69.7 ms per pass; width 4 moved from
75.3 to 71.7 ms. Every measured pass reused all 4,097 records without new
publication or revision allocation. The candidate also passed the real
encrypted-daemon reuse and reopen test and the focused indexing, sibling
publication, and cancellation tests. This establishes a local mechanism,
which the representative pair below tests directly.

One ordered control/candidate pair read `/ncl1-1-vs-50/fme_cae1` directly
with the same sealed parent, selected input hash, native daemon, group width
64, 56 blob tasks, 32 metadata-read slots, 1 ms coalescing and trace markers.
The control Go overlay restored only the repeated path calculations. Both
legs passed filtered restore, encrypted reopen and source identity checks;
each reused 799,076 records, changed none, issued no directory publication
calls and added no data blobs. Neither touched the production repository.

| Measurement | Control | Cached paths |
| --- | ---: | ---: |
| Main backup | 287.240 s | 275.420 s |
| Command elapsed | 294.390 s | 282.545 s |
| Directory writer elapsed | 31.270 s | 19.077 s |
| Compatibility overlap across no-read gaps | 10.478 s | 0.591 s |
| Prefetch overlap across no-read gaps | 15.235 s | 12.784 s |
| Longest gap with no matched read in flight | 18.500 s | 11.628 s |
| Compatibility overlap in longest gap | 5.780 s | 0.347 s |
| Total time with no matched read in flight | 134.067 s | 142.918 s |

Each leg matched its authenticated command window exactly: 152,305 reads
and 40,753,978,820 versus 40,753,454,576 bytes. Size RPCs were 85,146 versus
86,773, so identical input does not mean identical read traffic. The 94%
reduction in measured compatibility overlap and shorter directory phase
support the path-caching mechanism on NCL1. Main elapsed was 11.820 s (4.1%)
lower in this pair; fixed order, cache state and background load limit that
estimate. Neither leg passed the 600 s sustained qualification gate. No
matched read in flight does not imply an idle NAS or daemon, and the trace
does not measure native scheduler time. Prefetch is now the largest measured
directory overlap; these results do not justify raising concurrency caps.

Both `DIAGNOSTIC-SHA256SUMS` and leg-local `FOLLOWUP-SHA256SUMS` were checked
from their respective roots. The sealed series are under
`/volume2/NASDA2/vaultic-test-m6/ncl1-admission-fme-cae1-20261120-800k-dir-split-control`
and `/volume2/NASDA2/vaultic-test-m6/ncl1-admission-fme-cae1-20261120-800k-dir-split-candidate`.
The local replay reports and paired summary are in
`/run/vaultic-directory-pair-inputs-20261120`.

The follow-up directory prefetch attribution leg used the same direct source,
input hash, sealed parent, group width, concurrency, cache settings and
native daemon. It matched 152,302 positional reads and 40,753,192,340 bytes
exactly to the authenticated command window. Restore, reopen, reuse of all
799,076 records, zero new data and directory publications, both checksum
seals and runtime cleanup verified. This was an attribution leg, not a
matched throughput comparison.

| Directory prefetch stage | Calls | Full width (64) | Found / requested | Caller elapsed | RPC elapsed | Server storage elapsed |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Pointer | 1,885 | 1,867 | 120,025 / 120,025 | 5.659 s | 5.566 s | 4.662 s |
| Revision | 1,885 | 1,867 | 120,025 / 120,025 | 6.711 s | 6.631 s | 5.536 s |

Both stages had zero failures and a validated timing sample for every call;
their counts matched the same-writer prefetch trace regions. Server storage
was 83.8% and 83.5% of RPC elapsed, respectively. The two stages had 1.978 s
outside the server handler combined; eliminating a round trip would not by
itself remove the 10.198 s measured inside storage. The 99.0% full batches
also leave little room to improve occupancy at width 64. Finding a record
does not imply a cache hit: the 769,853 block hits and 443,374 misses cover
the whole command and cannot be assigned to either stage. Server storage
elapsed includes engine work and waits, not just physical I/O.

Prefetch overlapped 6.416 s of the longest 10.982 s gap without a matched
read in flight. Across the command, size RPCs overlapped 74.807 s of
143.557 s in such gaps; this remains a separate investigation, not proof
of storage saturation. Before changing read policy, isolate per-stage block
reuse and physical reads on the encrypted fixture. Only advance a candidate
that reduces storage work or demonstrably overlaps independent work without
changing publication order, then use a reverse-order direct-source pair.
The sealed attribution series is under
`/volume2/NASDA2/vaultic-test-m6/ncl1-admission-fme-cae1-20261120-800k-dir-split-prefetch`;
the local receipts are in `/run/vaultic-directory-prefetch-inputs-20261121`.

The opt-in encrypted directory fixture ran one 64-key batch per stage and
repeated both stages, reopening the daemon to reverse their order. Each
window verified one authenticated engine MultiGet with 64 keys and no open
transaction or write intent. Local and private NASDA2 fixtures agreed:

| Window | SlateDB block hits / misses | Positional file reads, 16 MiB chunk cache | Positional file reads, chunk cache disabled |
| --- | ---: | ---: | ---: |
| Pointer, first touch in either order | 0 / 1 | 0 | 1 (13,731 B) |
| Revision, first touch in either order | 1 / 2 | 0 | 1 (13,731 B) |
| Pointer repeat | 1 / 0 | 0 | 0 |
| Revision repeat | 3 / 0 | 0 | 0 |

With the chunk cache enabled, each first-touch window hit the same layer
once without fetching an origin body. A SlateDB block miss therefore did
not imply a positional read even on NASDA2. These counters measure file
read calls, not NAS RPCs or physical media. Fresh daemon opens do not clear
the OS cache and can populate the chunk cache before the window starts.
The small fixture showed no order-dependent change in file work and does
not justify tuning the representative cache.

A same-clock replay of the sealed direct-source attribution leg classified
every matched positional read by its full syscall interval. Pointer windows
contained 30 reads (7,864,800 B, 0.016 s summed syscall time) and revision
windows contained 246 reads (64,491,360 B, 0.267 s). No read crossed a stage
boundary and none of these 276 reads overlapped a traced blob-size RPC.
The remaining 152,026 reads and 40,680,836,180 B were outside both stages;
the bins conserve the exact authenticated command totals. This is temporal
overlap, not a request ID join. Syscall intervals exclude work before entry
and after exit, and do not measure physical NAS media activity.

The command added 270,285 chunk-cache hits, 151,783 misses, 86,398 evictions
and 700 sampled refetches (against 1,632 sampled first fetches). These are
shared cache counters, not attributable per stage in this capture; the
refetch counters sample fingerprints and are not total fetch counts. The
replay does not show enough accounted file work inside directory batches to
prioritize reducing their file read count. To explain the remaining
directory storage elapsed, separate cached engine work and waits before
considering another cache knob. Investigate the much larger blob-size path
separately. The replay and report are retained at
`/run/vaultic-directory-file-overlap-20261121.py` and
`/run/vaultic-directory-prefetch-inputs-20261121/prefetch-file-overlap.json`.

A second replay split the union of each caller's windows into disjoint
wall-time bins. A matched syscall takes precedence over a native admission
interval for a joined read; the latter excludes first-on-thread reads that
cannot be joined, but all exact positional reads remain in the syscall bin.

| Caller windows | Total | Matched read active | Matched admission only | Neither observed |
| --- | ---: | ---: | ---: | ---: |
| Directory pointer | 5.657 s | 0.016 s | 0.002 s | 5.638 s |
| Directory revision | 6.708 s | 0.265 s | 0.018 s | 6.425 s |
| Blob-size RPC | 218.860 s | 144.053 s | 1.683 s | 73.124 s |

During blob-size RPC activity, mean actual matched read depth was 5.86
including zero-depth time, with peak 77. Mean RPC depth was 5.24 with
peak 32. The peaks need not coincide. The 85,988 authenticated blob-size
RPCs accounted for 1,140.864 s of summed RPC elapsed, of which 1,078.801 s
(94.6%) was server storage. These are overlapping worker times, not wall
time or additive to the table. The uncovered wall time is not proof of
CPU work, source limitation, or idle storage. Source I/O timing here is
also cumulative, without aligned source activity intervals.

The native database-read timer includes taking the database read lock and
awaiting the SlateDB read. It does not expose per-stage lock wait, cache
processing, or unobserved async waits. Separating those components, and
aligning source intervals with the blob-size windows, would distinguish
the remaining hypotheses. The observed admission-only time does not
justify changing queue limits. The disjoint replay report is retained at
`/run/vaultic-directory-prefetch-inputs-20261121/prefetch-and-size-activity.json`.

The next direct NCL1 attribution leg added same-clock source payload read
regions and split the native database timer for authenticated directory
MultiGets. It ran the same forced 800k input in 272.787 s of main backup
time and matched 152,307 positional reads and 40,754,503,140 bytes exactly
to the command counters. Both seals, restore, encrypted reopen, source
identity, and runtime cleanup verified. The run reused all 799,076 records
with zero new data blobs or directory publications and did not modify the
production repository.

| Directory stage | Validated samples | Storage elapsed | Outer read lock wait | SlateDB MultiGet elapsed |
| --- | ---: | ---: | ---: | ---: |
| Pointer | 1,885 | 5.267 s | 0.924 ms | 5.240 s |
| Revision | 1,885 | 6.096 s | 0.857 ms | 6.039 s |

Each stage requested and found 120,025 keys, with 1,867 full-width batches
and no failures. The outer read lock took only 1.781 ms of the combined
11.363 s storage elapsed, versus 11.279 s inside the SlateDB await. That
rules out the outer lock as a meaningful target in this capture, not
internal engine contention. Engine elapsed includes execution and waits.

The source trace matched all 2,742,821 payload read calls. Their wall-time
union was 165.469 s. Within the 216.114 s union of blob-size RPC windows,
the replay separated source payload reads from matched NASDA2 positional
reads on the same monotonic clock:

| Observed activity during a blob-size RPC | Wall time |
| --- | ---: |
| Both source payload and NASDA2 positional reads | 92.642 s |
| Source payload reads only | 54.406 s |
| NASDA2 positional reads only | 52.056 s |
| Neither observed | 17.010 s |

Source reads overlapped 68.0% of active blob-size RPC time, including
54.406 s of the 71.416 s with no matched positional read in flight. Source
regions cover elapsed payload read calls, including scheduling, not source
traversal or physical I/O. A missing matched positional read does not mean
metadata processing stopped. Overlap does not establish a dependency or
identify the throughput limiter. This short unpaired capture does not
qualify a speedup or saturation claim. Prioritize profiling the larger
blob-size storage path over outer-lock or directory origin-read tuning.

The sealed capture is under
`/volume2/NASDA2/vaultic-test-m6/ncl1-admission-fme-cae1-20261120-800k-dir-split-source-db/leg-01-source-db`.
Local receipts, including `source-db-summary.json` and
`three-way-overlap.json`, are in `/run/vaultic-source-db-inputs-20261121`.

The sealed leg's perf events contain positional syscall and NFS tracepoints,
not CPU samples. They cannot explain the 17.010 s with neither observed
read type. An opt-in local encrypted blob-record fixture instead isolates
authenticated nontransactional MultiGets at widths 1, 5, and 16. The sealed
leg averaged 5.02 handles per size RPC; the fixture does not reproduce its
key distribution, storage layout, or concurrent load.

Across two fixture runs, first-touch batches at every width missed the
SlateDB block cache but made no positional read with the 16 MiB encrypted
chunk cache. Disabling only that cache yielded one positional read of
about 38.6 kB at each width. Repeats had no block misses or file reads in
either configuration. Daemon reopen is not an OS-cache reset.

A capped userspace cycles profile of 20,000 warm five-key batches verified
100,000 keys, 20,000 block hits, and no block misses or file reads. The
warm window covered 5.725 s; authenticated samples summed 1.253 s inside
SlateDB and 4.211 ms waiting for the outer read lock. Sampled execution
included SlateDB key routing and in-block seeks as well as buffer and
allocation work. These are CPU observations on a local cache-resident
fixture; neither the cycle profile nor the engine elapsed split measures
off-CPU waits in the NCL1 run. The profile and clock calibration are in
`/run/vaultic-blob-size-profile.8K5xbV`.

Do not advance a cache, lock, or concurrency change on this evidence.
A follow-up local encrypted fixture (`VAULTICDB_TEST_BLOB_SIZE_READS=1`
and `VAULTICDB_TEST_BLOB_SIZE_DISPERSED=1`) issued the same 800 five-key
batches in each first-pass and repeat window. It reopened the daemon for
each leg in depth order 1, 32, 32, 1, with the 16 MiB chunk cache enabled.
Each window verified 4,000 ordered results and authenticated timings, one
engine batch per RPC, and no committed writes. Across the three layouts,
mean client RPC overlap was 0.99 at peak 1 and 30.89-31.62 at peak 32.
The higher depth completed more batches per second despite higher p99
call latency. This is RPC overlap, not NASDA2 positional read depth.

| Private layout | Visits per batch | Filter negatives per window | Candidate keys per window | Planned blocks per window | Repeat wall at depth 1 / 32 |
| --- | ---: | ---: | ---: | ---: | --- |
| Default SST target | 1 | Not recorded | 4,000 | 3,929 | 224-275 / 60 ms |
| 64 KiB target, multi-SST (`VAULTICDB_TEST_BLOB_SIZE_MULTI_SST=1`) | 7 | Not recorded | 28,000 | 24,516 | 340-360 / 54-60 ms |
| 192 KiB target, filter-active (`VAULTICDB_TEST_BLOB_SIZE_FILTERED=1`) | 3 | 4,763 | 7,237 | 6,739 | 274-282 / 57-66 ms |

Within each layout, depth and cache warmth did not change visits, candidate
keys, or planned blocks. First-pass file reads were one 262,160-byte read
per default-layout leg and none in the other two layouts; all repeats
had zero misses and file reads. The seven-SST layout exercised substantial
speculative block work even with no observed file reads. Filter rejection
in the three-SST layout is real, but these layouts vary the number and
contents of tables, so their difference is not a filter speedup estimate.

The sealed direct-NCL1 command averaged 23.24 visits over 109,043 engine
MultiGets and recorded 19.55 million filter negatives. Those totals cover
all read types, not just blob sizes. Use request-scoped counts to separate
SSTs probed, filter rejections, and blocks planned for blob-size requests
before choosing a candidate. A matched direct NCL1 qualification still
needs a candidate that reduces measured work without changing results or
publication behavior.

The request-scoped diagnostic overlay is built from private copies of the
pinned SlateDB and native daemon sources; it does not change the production
engine pin. With `VAULTICDB_DIAGNOSTIC_BLOB_WORK=1`, successful native blob
MultiGets return numeric counts for SST visits, filter negatives, and
planned blocks. The counters follow the inline MultiGet future, not the
worker thread. Backup size lookups accumulate only their own authenticated
samples in `critical_path.blob_lookup.size_rpc_work`; missing metadata
does not invalidate a read or synthesize a sample. The sample count must
be checked before interpreting the totals. Planned blocks are not reads.

In the private filter-active fixture, all eight windows through the pinned
transactional read session returned 800 samples each. Every window's
sample sums exactly matched the independent engine status deltas: 2,400
SST visits, 4,762 filter negatives, and 6,740 planned blocks in this run.
That reconciliation held at peak RPC depths 1 and 32. The diagnostic
executable is `/run/vaultic-blob-work.lR4Xwe/vaulticdb-pinned-session`.

The guarded direct-NCL1 retry (`leg-02-blob-work`) returned 85,670 work
samples, exactly matching the size RPC trace count. Those RPCs accounted
for 2,263,596 SST visits (88.33% of the command delta), 3,705,064 filter
negatives (18.95%), and 451,056 planned blocks (39.30%). The exact NASDA2
replay matched 152,307 reads and 40,754,503,140 bytes. The first attempt
was rejected because the launcher scrubbed the diagnostic opt-in; it is
not a zero-work observation.

Wider batches reduced visits per key in the private fixture, but longer
coalescing delays and ready-only dispatch did not reliably improve local
CLI time. The exact catalog eliminated size RPCs and reduced mean full
command time from 4.130 s to 3.890 s in the checksummed 32,768-file local
screen. All four legs restored exactly and wrote no new data blobs.

The direct-NCL1 catalog control passed at 294.137 s command wall with
87,183 size RPCs. The catalog candidate hit the unchanged 1,200 s limit
without an accepted result. The trace places the CLI in startup catalog
construction: it scans the entire blob keyspace and inserts each decoded
page into the encrypted RAM working store before the crawl. Process
counters show about 9.6 GB transferred from daemon to CLI; the working
policy reported no disk spill. They do not isolate daemon iteration,
client decoding, and insertion costs. Retain on-demand lookup at this
scale. A bounded catalog-only probe can report these costs separately;
an on-demand read-ahead experiment should prioritize discovered blob IDs
and report useful hits and foreground wait rather than target full coverage.

All 46 legs, the read-depth replay, and the sweep inputs are retained under
`/volume2/NASDA2/vaultic-test-m6/nasda2-headroom-20261118-v0ruxc9f`. Verify
`SHA256SUMS` from that directory. The disposable write payloads were removed;
the retained object corpus and historical backup receipts were not changed.

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